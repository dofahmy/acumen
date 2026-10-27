# -*- coding: utf-8 -*-
"""Restored AMES and EGTS dashboard preserving original 13-column views.
TradingView staged tables only; legacy RS interface/routes removed.
"""
import csv, io, os, sys, subprocess, threading, time, logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, Response, jsonify, render_template_string, request, send_file
from sqlalchemy import create_engine, text
BASE=Path(__file__).resolve().parent
DATA_DIR=Path(os.environ.get('DATA_DIR',str(BASE))).resolve()
DATA_DIR.mkdir(parents=True,exist_ok=True)
app=Flask(__name__)
app.json.ensure_ascii=False
LOCK=threading.Lock()
JOBS={'update':{'running':False,'ok':None,'message':'جاهز','log':''}}
def run_job(kind,command):
    with LOCK:
        if JOBS[kind]['running']:return False
        JOBS[kind].update(running=True,ok=None,message='بدأ التشغيل',log='')
    def work():
        try:
            p=subprocess.Popen(command,cwd=str(BASE),env=os.environ.copy(),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8',errors='replace',bufsize=1)
            for line in p.stdout:
                with LOCK:
                    job=JOBS[kind];job['log']=(job['log']+line)[-25000:];job['message']=line.strip()[:200] or job['message']
            rc=p.wait()
            with LOCK:JOBS[kind].update(running=False,ok=rc==0,message='اكتمل التحديث' if rc==0 else f'فشل التحديث (exit {rc})')
        except Exception as e:
            with LOCK:JOBS[kind].update(running=False,ok=False,message=str(e),log=JOBS[kind]['log']+'\n'+str(e))
    threading.Thread(target=work,daemon=True).start()
    return True
@app.get('/')
def index():return render_template_string(HTML)
# Cache confirmed latest fully CLOSED EGX30 trading session from TradingView.
# A fresh TradingView history probe runs in a subprocess to avoid blocking Flask.
TV_CHECK = {'checked_at': 0.0, 'checking': False, 'latest': None, 'error': None}
TV_CHECK_LOCK = threading.Lock()

def _tv_probe_worker():
    try:
        probe = r"""import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from tvDatafeed import TvDatafeed, Interval
bars=TvDatafeed().get_hist(symbol='EGX30',exchange='EGX',interval=Interval.in_daily,n_bars=8)
if bars is None or bars.empty: raise RuntimeError('TradingView did not provide EGX30 candles')
dates=sorted({x.date() for x in bars.index.to_pydatetime()})
now=datetime.now(ZoneInfo('Africa/Cairo'))
# Conservative: never use current-day candles before 15:00 Cairo, since incomplete.
if now.hour < 15: dates=[d for d in dates if d < now.date()]
else: dates=[d for d in dates if d <= now.date()]
if not dates: raise RuntimeError('No completed EGX30 sessions available')
print(dates[-1].isoformat())
"""
        candidates=[BASE/'.venv312'/'Scripts'/'python.exe',Path(sys.executable)]
        exe=next((str(p) for p in candidates if p.is_file()),sys.executable)
        # Prefer the active environment if tvDatafeed lives there.
        for candidate in [Path(sys.executable),BASE/'.venv312'/'Scripts'/'python.exe']:
            if not candidate.is_file():continue
            try:
                test=subprocess.run([str(candidate),'-c','import tvDatafeed'],capture_output=True,timeout=6)
                if test.returncode==0:exe=str(candidate);break
            except Exception:pass
        result=subprocess.run([exe,'-c',probe],capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=24)
        if result.returncode:raise RuntimeError((result.stderr or result.stdout or 'TradingView probe failed')[-240:])
        latest=result.stdout.strip().splitlines()[-1].strip()
        datetime.strptime(latest,'%Y-%m-%d')
        with TV_CHECK_LOCK: TV_CHECK.update(latest=latest,error=None,checked_at=time.monotonic(),checking=False)
    except Exception as exc:
        with TV_CHECK_LOCK: TV_CHECK.update(latest=None,error=str(exc)[:240],checked_at=time.monotonic(),checking=False)

@app.get('/api/update/availability')
def tv_update_availability():
    try:
        url=os.environ.get('DATABASE_URL')
        if not url: raise RuntimeError('DATABASE_URL missing')
        eng=create_engine(url,pool_pre_ping=True,connect_args={'options':'-c default_transaction_read_only=on'})
        try:
            with eng.connect() as conn:
                local_stock=conn.execute(text('SELECT MAX(session_date) FROM public.eg_tv_stocks_1d')).scalar()
                local_index=conn.execute(text("SELECT MAX(session_date) FROM public.eg_tv_indices_1d WHERE symbol='EGX30'")).scalar()
        finally: eng.dispose()
        local_stock=str(local_stock) if local_stock else None
        local_index=str(local_index) if local_index else None
        with LOCK: running=JOBS['update']['running']
        with TV_CHECK_LOCK:
            age=time.monotonic()-TV_CHECK['checked_at']
            if not TV_CHECK['checking'] and (TV_CHECK['checked_at']==0 or age>(300 if TV_CHECK['error'] is None else 90)):
                TV_CHECK['checking']=True
                threading.Thread(target=_tv_probe_worker,daemon=True).start()
            checking=TV_CHECK['checking']
            remote=TV_CHECK['latest']
            error=TV_CHECK['error']
        # If not verified, do NOT claim data is current or allow a speculative update.
        needs=bool(remote and ((not local_stock or remote>local_stock) or (not local_index or remote>local_index)))
        current=bool(remote and local_stock and local_index and local_stock>=remote and local_index>=remote)
        return jsonify(ok=True,stored=local_stock,stored_index=local_index,available=remote,
                       checking=checking,needs_update=needs,up_to_date=current,
                       running=running,error=error)
    except Exception as exc:
        return jsonify(ok=False,error=str(exc),stored=None,available=None,needs_update=False,up_to_date=False,checking=False,running=False),503

@app.get('/api/data')
def data():
    eng=None
    try:
        if not os.getenv('DATABASE_URL'):raise RuntimeError('DATABASE_URL missing')
        eng=create_engine(os.environ['DATABASE_URL'],pool_pre_ping=True,connect_args={'options':'-c default_transaction_read_only=on'})
        with eng.connect() as c:
            if c.execute(text('SELECT current_database()')).scalar()!='stocks_local':raise RuntimeError('Wrong database')
            stats=c.execute(text('SELECT COUNT(DISTINCT symbol),MAX(session_date) FROM public.eg_tv_stocks_1d')).one()
            inds=c.execute(text("SELECT symbol,MAX(session_date) FROM public.eg_tv_indices_1d WHERE symbol IN ('EGX30','EGX70EWI','EGX100EWI') GROUP BY symbol")).all()
        return jsonify(ok=True,symbols=stats[0],latest=str(stats[1]),indices={r[0]:str(r[1]) for r in inds})
    except Exception as e:return jsonify(ok=False,error=str(e)),500
    finally:
        if eng is not None:eng.dispose()
# Independent AMES-style color-cycle screener; original RS pages remain unchanged.
AMES_SCRIPT = BASE / 'egx_ames_pattern_all_2026.py'
AMES_OUT = BASE / 'EGX_AMES_PATTERN_2026'
ames_lock = threading.Lock()
ames_status = {'running': False, 'ok': None, 'message': 'جاهز', 'log': '', 'started_at': None}

def selected_dates(payload):
    start=str(payload.get('start','')).strip()
    end=str(payload.get('end','')).strip()
    try:
        a=datetime.strptime(start,'%Y-%m-%d').date()
        b=datetime.strptime(end,'%Y-%m-%d').date()
        if not (2019<=a.year<=2100 and 2019<=b.year<=2100) or a>b:
            raise ValueError()
    except (TypeError,ValueError):
        raise ValueError('اختاري تاريخ بداية ونهاية صحيحين؛ البداية قبل النهاية')
    return start,end

def filter_dates(rows):
    start=request.args.get('start','')
    end=request.args.get('end','')
    if start or end:
        selected_dates({'start':start,'end':end})
    return [r for r in rows if (not start or r.get('signal_date','')>=start) and (not end or r.get('signal_date','')<=end)]

def ames_worker(start,end):
    cmd = [sys.executable, '-u', str(AMES_SCRIPT), '--period', '50', '--price-sma', '7',
           '--gray-sma', '50', '--min-below', '50', '--start', start,
           '--end', end, '--output', str(AMES_OUT)]
    lines = []
    try:
        p = subprocess.Popen(cmd, cwd=str(BASE), env=os.environ.copy(), stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
        for line in p.stdout:
            lines.append(line.rstrip())
            with ames_lock:
                ames_status['log'] = 'بدء فحص AMES\n' + '\n'.join(lines[-80:])
                ames_status['message'] = line.strip()[:180] or 'جارٍ الفحص'
        rc=p.wait()
        with ames_lock:
            ames_status.update(running=False,ok=(rc==0),message=('اكتمل الفحص' if rc==0 else 'فشل الفحص (رمز '+str(rc)+') — افتحي سجل تشغيل AMES'))
    except Exception as exc:
        with ames_lock:
            ames_status.update(running=False,ok=False,message=str(exc),log='\n'.join(lines[-80:])+'\n'+str(exc))

@app.get('/api/ames/status')
def ames_get_status():
    with ames_lock:
        return jsonify({**ames_status,'script_exists':AMES_SCRIPT.is_file(),'elapsed_seconds':int(time.time()-ames_status['started_at']) if ames_status['running'] and ames_status.get('started_at') else 0,'saved_exists':(AMES_OUT/'EGX_ALL_BUY_SIGNALS_2026.csv').is_file()})

@app.post('/api/ames/run')
def ames_run():
    data=request.get_json(silent=True) or {}
    try:
        start,end=selected_dates(data)
    except (ValueError,TypeError) as e:return jsonify({'error':str(e)}),400
    if not AMES_SCRIPT.is_file():return jsonify({'error':'ملف egx_ames_pattern_all_2026.py غير موجود بجانب ملف الويب'}),400
    if not os.environ.get('DATABASE_URL'):return jsonify({'error':'DATABASE_URL غير مضبوط في بيئة تشغيل الويب'}),400
    with ames_lock:
        if ames_status['running']:return jsonify({'error':'فحص AMES يعمل بالفعل'}),409
        ames_status.update(running=True,ok=None,message='بدأ فحص AMES',log='بدء تشغيل الفحص؛ قد يستغرق تحميل التاريخ من PostgreSQL بعض الوقت...\n',started_at=time.time())
    threading.Thread(target=ames_worker,args=(start,end),daemon=True).start()
    return jsonify({'started':True,'start':start,'end':end}),202

# Market OHLC enrichment for AMES results. Read-only official TradingView data only.
_ames_bars_lock = threading.Lock()
_ames_bars_cache = {'until': 0, 'values': {}, 'error': None, 'mtime': None}

def ames_stock_bars():
    now = time.monotonic()
    with _ames_bars_lock:
        signal_file = AMES_OUT/'EGX_ALL_BUY_SIGNALS_2026.csv'
        mtime = signal_file.stat().st_mtime_ns if signal_file.is_file() else None
        if now < _ames_bars_cache['until'] and mtime == _ames_bars_cache['mtime']:
            return _ames_bars_cache['values'], _ames_bars_cache['error']
        engine = None
        try:
            url = os.getenv('DATABASE_URL')
            if not url:
                raise RuntimeError('DATABASE_URL is missing')
            engine = create_engine(url, pool_pre_ping=True,
                connect_args={'options': '-c default_transaction_read_only=on'})
            # Load the same complete adjusted TradingView stock history used by
            # the scanner itself. Do NOT restrict symbols to a stale year/run.
            signal_file = AMES_OUT/'EGX_ALL_BUY_SIGNALS_2026.csv'
            if not signal_file.is_file():
                return {}, None
            with signal_file.open(encoding='utf-8-sig', newline='') as f:
                symbols = sorted({r['symbol'].strip() for r in csv.DictReader(f)
                                  if r.get('symbol') and r['symbol'].strip()})
            if not symbols:
                return {}, None
            stmt = text("""SELECT symbol, session_date, h, c
                           FROM public.eg_tv_stocks_1d
                           ORDER BY symbol, session_date""")
            by_symbol = {sym: [] for sym in symbols}
            with engine.connect() as conn:
                for r in conn.execute(stmt):
                    key = str(r.symbol).strip()
                    if key in by_symbol:
                        by_symbol[key].append((str(r.session_date)[:10],
                            float(r.h) if r.h is not None else None,
                            float(r.c) if r.c is not None else None))
            missing = [sym for sym in symbols if not by_symbol[sym]]
            if missing:
                app.logger.warning('AMES OHLC missing for %d/%d symbols: %s',
                                   len(missing), len(symbols), ','.join(missing))
            _ames_bars_cache.update(values=by_symbol,error=('لا توجد شموع TradingView في الجدول لهذه الرموز: '+', '.join(missing)) if missing else None,until=time.monotonic()+60,mtime=mtime)
        except Exception as exc:
            err=f'{type(exc).__name__}: {exc}'
            app.logger.warning('AMES read-only OHLC lookup failed: %s',err)
            _ames_bars_cache.update(values={},error=err,until=time.monotonic()+15,mtime=mtime)
        finally:
            if engine is not None: engine.dispose()
        return _ames_bars_cache['values'],_ames_bars_cache['error']

def ames_enriched_rows():
    path=AMES_OUT/'EGX_ALL_BUY_SIGNALS_2026.csv'
    if not path.is_file(): return [],None
    with path.open(encoding='utf-8-sig',newline='') as f: rows=list(csv.DictReader(f))
    prices,err=ames_stock_bars()
    from bisect import bisect_left, bisect_right
    for r in rows:
        r.update(high52_before=None, high52_before_date=None, peak52_gain_pct=None, peak_after=None,
                 peak_after_date=None, sessions_to_peak=None, latest_close=None,
                 latest_date=None, peak_gain_pct=None)
        bars=prices.get(r.get('symbol',''),[])
        if not bars: continue
        ds=[b[0] for b in bars]
        signal_day=r.get('signal_date','')
        if not signal_day: continue
        k=bisect_left(ds,signal_day)
        begin=(datetime.strptime(signal_day,'%Y-%m-%d')-timedelta(weeks=52)).strftime('%Y-%m-%d')
        earlier=[b for b in bars[bisect_left(ds,begin):k] if b[1] is not None and b[1]>0]
        if earlier:
            top=max(earlier,key=lambda b:b[1])
            r['high52_before']=top[1]
            r['high52_before_date']=top[0]
        # Only observed sessions strictly after the signal and within 52 calendar weeks.
        # No forward filling, synthetic bars, or extrapolation beyond the latest data.
        end52=(datetime.strptime(signal_day,'%Y-%m-%d')+timedelta(weeks=52)).strftime('%Y-%m-%d')
        after_idx=bisect_right(ds,signal_day)
        future=[(i,b) for i,b in enumerate(bars[after_idx:bisect_right(ds,end52)],start=after_idx) if b[1] is not None and b[1]>0]
        if future:
            peak_idx, peak_bar=max(future,key=lambda it:it[1][1])
            r['peak_after']=peak_bar[1]
            r['peak_after_date']=peak_bar[0]
            r['sessions_to_peak']=peak_idx-bisect_right(ds,signal_day)+1
            try:
                entry=float(r.get('close') or 0)
                if entry>0:
                    r['peak_gain_pct']=(peak_bar[1]/entry-1)*100
                    r['peak52_gain_pct']=r['peak_gain_pct']
            except (TypeError,ValueError):pass
        recent=next((b for b in reversed(bars) if b[2] is not None and b[2]>0),None)
        if recent:
            r['latest_close']=recent[2]
            r['latest_date']=recent[0]
    return rows,err

@app.get('/api/ames/signals')
def ames_signals():
    rows,err=ames_enriched_rows()
    query=request.args.get('q','').upper().strip()
    try:rows=filter_dates(rows)
    except ValueError as e:return jsonify({'error':str(e)}),400
    if query:rows=[r for r in rows if query in r.get('symbol','').upper()]
    rows.sort(key=lambda r:(r.get('signal_date',''),r.get('symbol','')),reverse=True)
    return jsonify({'rows':rows,'total':len(rows),'message':'OK','price_error':err})

@app.get('/download/ames-signals.csv')
def ames_download():
    rows,err=ames_enriched_rows()
    if not (AMES_OUT/'EGX_ALL_BUY_SIGNALS_2026.csv').is_file():
        return jsonify({'error':'شغّلي فحص AMES أولًا'}),404
    if err:return jsonify({'error':'تعذر تحميل أسعار TradingView: '+err}),503
    cols=['symbol','signal_date','signal_color','below_sessions','cross_down_date',
          'cross_up_date','sessions_cross_up_to_buy','close','high52_before',
          'high52_before_date','peak52_gain_pct','peak_after','peak_after_date','sessions_to_peak',
          'peak_gain_pct','peak52_gain_pct','latest_close','latest_date']
    f=io.StringIO();f.write('\ufeff')
    w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)
    return Response(f.getvalue(),mimetype='text/csv; charset=utf-8',headers={
        'Content-Disposition':'attachment; filename=AMES_PATTERN_WITH_PRICES.csv'})


# Independent EGTS system. The AMES routes and scanner remain untouched.
EGTS_SCRIPT=BASE/'egx_egts_pattern_market.py'
EGTS_OUT=BASE/'EGX_EGTS_PATTERN'
egts_lock=threading.Lock()
egts_status={'running':False,'ok':None,'message':'جاهز','log':''}

def egts_worker(start,end):
    cmd=[sys.executable,'-u',str(EGTS_SCRIPT),'--start',start,'--end',end,'--max-after-up','35','--output',str(EGTS_OUT)]
    try:
        p=subprocess.Popen(cmd,cwd=str(BASE),env=os.environ.copy(),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8',errors='replace')
        for line in p.stdout:
            with egts_lock:
                egts_status['log']=(egts_status['log']+line)[-14000:]
                egts_status['message']=line.strip()[:200] or 'جارٍ الفحص'
        rc=p.wait()
        with egts_lock:egts_status.update(running=False,ok=(rc==0),message='اكتمل الفحص' if rc==0 else f'فشل الفحص (exit {rc})')
    except Exception as e:
        with egts_lock:egts_status.update(running=False,ok=False,message=str(e),log=egts_status['log']+'\n'+str(e))

@app.get('/api/egts/status')
def egts_status_get():
    with egts_lock:return jsonify(dict(egts_status))

@app.post('/api/egts/run')
def egts_run():
    try:
        start,end=selected_dates(request.get_json(silent=True) or {})
    except (ValueError,TypeError) as e:return jsonify({'error':str(e)}),400
    if not EGTS_SCRIPT.is_file():return jsonify({'error':'ملف egx_egts_pattern_market.py غير موجود'}),400
    if not (BASE/'egx_rps_three_patterns.py').is_file():return jsonify({'error':'ملف egx_rps_three_patterns.py غير موجود'}),400
    if not os.environ.get('DATABASE_URL'):return jsonify({'error':'DATABASE_URL غير مضبوط'}),400
    with egts_lock:
        if egts_status['running']:return jsonify({'error':'الفحص يعمل بالفعل'}),409
        egts_status.update(running=True,ok=None,message='بدأ فحص EGTS',log='')
    threading.Thread(target=egts_worker,args=(start,end),daemon=True).start()
    return jsonify({'started':True,'start':start,'end':end}),202

def egts_enriched():
    fp=EGTS_OUT/'EGTS_SIGNALS.csv'
    if not fp.is_file():return [],None
    with fp.open(encoding='utf-8-sig',newline='') as f:rows=list(csv.DictReader(f))
    if not rows:return [],None
    from bisect import bisect_left,bisect_right
    by_symbol={}
    try:
        eng=create_engine(os.environ['DATABASE_URL'],pool_pre_ping=True,connect_args={'options':'-c default_transaction_read_only=on'})
        try:
            with eng.connect() as c:
                # Read price history only for symbols present in saved EGTS signals.
                # The old implementation scanned EVERY stock row on each filter click.
                from sqlalchemy import bindparam
                symbols=sorted({r['symbol'] for r in rows if r.get('symbol')})
                statement=text('''SELECT symbol,session_date,h,c FROM public.eg_tv_stocks_1d
                                 WHERE symbol IN :symbols ORDER BY symbol,session_date''').bindparams(bindparam('symbols',expanding=True))
                for r in c.execute(statement,{'symbols':symbols}):
                    by_symbol.setdefault(r.symbol,[]).append((str(r.session_date)[:10],float(r.h) if r.h is not None else None,float(r.c) if r.c is not None else None))
        finally:eng.dispose()
    except Exception as e:return rows,f'{type(e).__name__}: {e}'
    for r in rows:
        r.update(high52_before=None,peak52_gain_pct=None,peak_after_date=None,sessions_to_peak=None,latest_close=None,latest_date=None)
        bars=by_symbol.get(r['symbol'],[])
        if not bars:continue
        ds=[b[0] for b in bars];dt=r['signal_date'];ix=bisect_left(ds,dt)
        pre=(datetime.strptime(dt,'%Y-%m-%d')-timedelta(weeks=52)).strftime('%Y-%m-%d')
        earlier=[b for b in bars[bisect_left(ds,pre):ix] if b[1] is not None and b[1]>0]
        if earlier:r['high52_before']=max(b[1] for b in earlier)
        end=(datetime.strptime(dt,'%Y-%m-%d')+timedelta(weeks=52)).strftime('%Y-%m-%d')
        pos=bisect_right(ds,dt)
        future=[(j,b) for j,b in enumerate(bars[pos:bisect_right(ds,end)],start=pos) if b[1] is not None and b[1]>0]
        if future:
            j,b=max(future,key=lambda item:item[1][1]);r['peak_after_date']=b[0];r['sessions_to_peak']=j-pos+1
            entry=float(r.get('close') or 0)
            if entry>0:r['peak52_gain_pct']=(b[1]/entry-1)*100
        recent=next((b for b in reversed(bars) if b[2] is not None and b[2]>0),None)
        if recent:r['latest_date']=recent[0];r['latest_close']=recent[2]
    return rows,None

@app.get('/api/egts/signals')
def egts_signals():
    rows,err=egts_enriched();q=request.args.get('q','').upper().strip()
    try:rows=filter_dates(rows)
    except ValueError as e:return jsonify({'error':str(e)}),400
    rows=[r for r in rows if q in r['symbol'].upper()]
    return jsonify({'rows':rows,'total':len(rows),'price_error':err})

@app.get('/download/egts-signals.csv')
def egts_download():
    rows,err=egts_enriched()
    if err:return jsonify({'error':err}),503
    if not (EGTS_OUT/'EGTS_SIGNALS.csv').is_file():return jsonify({'error':'شغّلي فحص EGTS أولًا'}),404
    cols=['symbol','signal_date','signal_color','below_gray_sessions','cross_down_date','cross_up_date','zero_cross_up_date','sessions_cross_up_to_buy','close','high52_before','peak52_gain_pct','peak_after_date','sessions_to_peak','latest_close','latest_date']
    f=io.StringIO();f.write('\ufeff');w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)
    return Response(f.getvalue(),mimetype='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename=EGTS_PATTERN_WITH_PRICES.csv'})

# CCAP PGG independent system. AMES/EGTS functions are deliberately unchanged.
CCAP_SCRIPT = BASE / 'egx_ccap_purple_gray_green.py'
CCAP_OUT = DATA_DIR / 'EGX_CCAP_PURPLE_GRAY_GREEN'
ccap_lock = threading.Lock()
ccap_status = {'running':False,'ok':None,'message':'جاهز','log':''}
CCAP_DEFAULTS = {'rs':29,'ma_fast':18,'ma_slow':29}

def ccap_settings(source):
    values={}
    for k,default in CCAP_DEFAULTS.items():
        raw=source.get(k,default)
        try:
            if isinstance(raw,bool) or str(raw).strip()!=str(int(raw)):
                raise ValueError()
            values[k]=int(raw)
        except (TypeError,ValueError):
            raise ValueError('قيمة غير صحيحة: '+k)
    if not (20<=values['rs']<=150 and 3<=values['ma_fast']<=30 and 20<=values['ma_slow']<=150 and values['ma_fast']<values['ma_slow']):
        raise ValueError('RS من 20 إلى 150؛ القصير من 3 إلى 30؛ الطويل من 20 إلى 150؛ القصير أقل من الطويل')
    return values

def ccap_output(settings):
    if settings == {'rs':50,'ma_fast':7,'ma_slow':50}: return CCAP_OUT
    return DATA_DIR / ('EGX_CCAP_PGG_RS%d_F%d_L%d' % (settings['rs'],settings['ma_fast'],settings['ma_slow']))


def ccap_worker(settings):
    cmd=[sys.executable,'-u',str(CCAP_SCRIPT),'--output',str(ccap_output(settings)),'--rs',str(settings['rs']),'--ma-fast',str(settings['ma_fast']),'--ma-slow',str(settings['ma_slow'])]
    try:
        with subprocess.Popen(cmd,cwd=str(BASE),env=os.environ.copy(),stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,text=True,encoding='utf-8',errors='replace',bufsize=1) as proc:
            for line in proc.stdout:
                with ccap_lock:
                    ccap_status['log']=(ccap_status['log']+line)[-16000:]
                    ccap_status['message']=line.strip()[:160] or ccap_status['message']
            ret=proc.wait()
        with ccap_lock:
            ccap_status.update(running=False,ok=ret==0,message='اكتمل الفحص' if ret==0 else f'فشل الفحص (exit {ret})')
    except Exception as exc:
        with ccap_lock:
            ccap_status.update(running=False,ok=False,message=str(exc),log=ccap_status['log']+'\n'+str(exc))

@app.get('/api/ccap/status')
def ccap_get_status():
    try: settings=ccap_settings(request.args)
    except ValueError as exc:return jsonify(error=str(exc)),400
    with ccap_lock:
        return jsonify({**ccap_status,'script_exists':CCAP_SCRIPT.is_file(),
                        'saved_exists':(ccap_output(settings)/'ALL_MARKET_SIGNALS.csv').is_file()})

@app.post('/api/ccap/run')
def ccap_run():
    try: settings=ccap_settings(request.get_json(silent=True) or {})
    except ValueError as exc:return jsonify(error=str(exc)),400
    if not CCAP_SCRIPT.is_file():return jsonify(error='ملف egx_ccap_purple_gray_green.py غير موجود'),400
    if not os.getenv('DATABASE_URL'):return jsonify(error='DATABASE_URL غير مضبوط'),400
    with ccap_lock:
        if ccap_status['running']:return jsonify(error='فحص CCAP يعمل بالفعل'),409
        ccap_status.update(running=True,ok=None,message='بدأ الفحص',log='بدأ تحديث إشارات CCAP PGG...\n')
    threading.Thread(target=ccap_worker,args=(settings,),daemon=True).start()
    return jsonify(started=True),202

CCAP_RANGE_FIELDS = {
    'purple_sessions':'purple_sessions', 'gray_sessions':'gray_sessions',
    'sequence_sessions':'sequence_sessions','purple_min':'purple_min_rps',
    'peak_gain':'peak_gain_pct','peak_sessions':'sessions_to_peak',
    'current_return':'current_return_pct',
}

def ccap_filtered_rows():
    settings=ccap_settings(request.args)
    fp=ccap_output(settings)/'ALL_MARKET_SIGNALS.csv'
    if not fp.is_file():return [],False
    with fp.open(encoding='utf-8-sig',newline='') as fh:rows=list(csv.DictReader(fh))
    rows=filter_dates(rows)
    q=request.args.get('q','').strip().upper()
    conditions=[]
    for name,column in CCAP_RANGE_FIELDS.items():
        low=request.args.get(name+'_min','').strip()
        high=request.args.get(name+'_max','').strip()
        lo=float(low) if low else None
        hi=float(high) if high else None
        if any(x is not None and not __import__('math').isfinite(x) for x in (lo,hi)) or (lo is not None and hi is not None and lo>hi):
            raise ValueError('نطاق غير صحيح: '+name)
        conditions.append((column,lo,hi))
    result=[]
    for row in rows:
        if q not in row.get('symbol','').upper():continue
        valid=True
        for col,lo,hi in conditions:
            if lo is None and hi is None:continue
            try:val=float(row.get(col,''))
            except (ValueError,TypeError):valid=False;break
            if not __import__('math').isfinite(val) or (lo is not None and val<lo) or (hi is not None and val>hi):valid=False;break
        if valid:result.append(row)
    result.sort(key=lambda x:(x.get('signal_date',''),x.get('symbol','')),reverse=True)
    return result,True

@app.get('/api/ccap/signals')
def ccap_signals():
    try:rows,saved=ccap_filtered_rows()
    except ValueError as exc:return jsonify(error=str(exc)),400
    return jsonify(rows=rows,total=len(rows),saved_exists=saved)

@app.get('/download/ccap-signals.csv')
def ccap_download():
    try:rows,saved=ccap_filtered_rows()
    except ValueError as exc:return jsonify(error=str(exc)),400
    if not saved:return jsonify(error='شغلي فحص CCAP الجديد أولا'),404
    with (ccap_output(ccap_settings(request.args))/'ALL_MARKET_SIGNALS.csv').open(encoding='utf-8-sig',newline='') as fh:
        columns=csv.DictReader(fh).fieldnames or []
    output=io.StringIO();output.write('\ufeff')
    writer=csv.DictWriter(output,fieldnames=columns,extrasaction='ignore');writer.writeheader();writer.writerows(rows)
    return Response(output.getvalue(),mimetype='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename="CCAP_PGG_FILTERED.csv"'})

@app.post('/api/update/run')
def update_run():
 raw=os.getenv('TV_UPDATER_SCRIPT','').strip() or str(BASE/'egx_tv_daily_update.py')
 if not raw:return jsonify(error='زر التحديث يحتاج ربط ملف تنزيل TradingView المستخدم حاليًا عبر TV_UPDATER_SCRIPT؛ لن ندّعي تحديثًا لم يحدث.'),400
 path=Path(raw).expanduser().resolve()
 if not path.is_file() or path.suffix.lower()!='.py':return jsonify(error='ملف تحديث TradingView غير موجود أو ليس Python'),400
 # Prefer current Python; older TradingView environment was .venv312 on this laptop.
 candidates=[Path(sys.executable), BASE/'.venv312'/'Scripts'/'python.exe']
 selected=None
 for candidate in candidates:
  if not candidate.is_file():continue
  try:
   check=subprocess.run([str(candidate),'-c','from tvDatafeed import TvDatafeed, Interval; import sqlalchemy, pandas'],capture_output=True,text=True,timeout=12)
   if check.returncode==0:
    selected=str(candidate);break
  except (OSError,subprocess.TimeoutExpired):pass
 if selected is None:
  return jsonify(error='مكتبة tvdatafeed غير موجودة في Python المستخدم. افتحي PowerShell وشغّلي: cd D:\\egx\\Rashid ثم python -m pip install tvdatafeed (أو استخدمي .venv312 المثبّت عليها المكتبة).'),400
 ok=run_job('update',[selected,'-u',str(path)])
 return (jsonify(started=True),202) if ok else (jsonify(error='تحديث يعمل بالفعل'),409)
@app.get('/download/<kind>.csv')
def download(kind):
 paths={'egts':EGTS_OUT/'EGTS_SIGNALS.csv','ames':AMES_OUT/'EGX_ALL_BUY_SIGNALS_2026.csv'}
 if kind not in paths:return jsonify(error='غير متاح'),404
 if not paths[kind].is_file():return jsonify(error='لا يوجد ملف نتائج بعد'),404
 return send_file(paths[kind],as_attachment=True)

@app.get('/api/update/status')
def tv_update_status():
    with LOCK:return jsonify(dict(JOBS['update']))


# CCAP named filter presets. Local JSON file; no market/database modifications.
CCAP_PRESETS_FILE = DATA_DIR / 'CCAP_PGG_SAVED_FILTER_PRESETS.json'
ccap_preset_lock = threading.Lock()
CCAP_PRESET_IDS = ('ccapStart','ccapEnd','ccapSearch') + tuple(
    'ccap_'+key+'_'+side for key in ('purple_sessions','gray_sessions','sequence_sessions','purple_min','peak_gain','peak_sessions','current_return')
    for side in ('min','max'))

def _read_ccap_presets():
    if not CCAP_PRESETS_FILE.exists():
        return {}
    data = json.loads(CCAP_PRESETS_FILE.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('Invalid saved presets file')
    return data

def _write_ccap_presets(presets):
    import tempfile
    # Atomic replacement: a partial write cannot corrupt the previous presets.
    with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=str(BASE),
                                     prefix='.ccap_presets_',suffix='.tmp',delete=False) as f:
        tmp=Path(f.name)
        json.dump(presets,f,ensure_ascii=False,indent=2)
        f.flush()
        os.fsync(f.fileno())
    try:os.replace(tmp,CCAP_PRESETS_FILE)
    finally:
        if tmp.exists():tmp.unlink()

@app.get('/api/ccap/presets')
def ccap_get_presets():
    try:
        with ccap_preset_lock:data=_read_ccap_presets()
        return jsonify(presets=data)
    except (OSError,ValueError) as exc:
        return jsonify(error='تعذر قراءة الإعدادات المحفوظة: '+str(exc)),500

@app.post('/api/ccap/presets')
def ccap_save_preset():
    data=request.get_json(silent=True) or {}
    name=data.get('name','')
    vals=data.get('values')
    if not isinstance(name,str) or not 1<=len(name.strip())<=60 or any(ord(c)<32 for c in name):
        return jsonify(error='اسم المجموعة يجب أن يكون من 1 إلى 60 حرفًا'),400
    name=name.strip()
    if not isinstance(vals,dict) or set(vals)-set(CCAP_PRESET_IDS):
        return jsonify(error='حقول الإعدادات غير صحيحة'),400
    if any(not isinstance(v,str) or len(v)>160 for v in vals.values()):
        return jsonify(error='قيمة إعدادات غير صحيحة'),400
    try:
        from datetime import date
        start=date.fromisoformat(vals.get('ccapStart',''))
        end=date.fromisoformat(vals.get('ccapEnd',''))
        if start>end or not (2019<=start.year<=2100 and 2019<=end.year<=2100):raise ValueError()
        
        for key in ('purple_sessions','gray_sessions','sequence_sessions','purple_min','peak_gain','peak_sessions','current_return'):
            lo=vals.get('ccap_'+key+'_min',''); hi=vals.get('ccap_'+key+'_max','')
            if lo!='' and not __import__('math').isfinite(float(lo)):raise ValueError()
            if hi!='' and not __import__('math').isfinite(float(hi)):raise ValueError()
            if lo!='' and hi!='' and float(lo)>float(hi):raise ValueError()
    except (ValueError,TypeError,OverflowError):
        return jsonify(error='تأكدي من صحة التواريخ وحدود الفلاتر'),400
    try:
        with ccap_preset_lock:
            presets=_read_ccap_presets()
            presets[name]={k:vals.get(k,'') for k in CCAP_PRESET_IDS}
            _write_ccap_presets(presets)
        return jsonify(ok=True,name=name)
    except (OSError,ValueError) as exc:
        return jsonify(error='تعذر حفظ المجموعة: '+str(exc)),500

@app.delete('/api/ccap/presets/<path:name>')
def ccap_delete_preset(name):
    try:
        with ccap_preset_lock:
            presets=_read_ccap_presets()
            if name not in presets:return jsonify(error='المجموعة غير موجودة'),404
            del presets[name]
            _write_ccap_presets(presets)
        return jsonify(ok=True)
    except (OSError,ValueError) as exc:
        return jsonify(error='تعذر حذف المجموعة: '+str(exc)),500

HTML='<!doctype html><html lang="ar" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>نظام القوة النسبية لسوق المال المصري</title><style>\n:root{--bg:#0d1422;--panel:#141f30;--panel2:#1a2940;--border:#293953;--text:#e8f0fa;--muted:#93a8c0;--green:#32d5a3;--amber:#f8bd62;--blue:#60a5fa;--red:#fa7784}*{box-sizing:border-box}body{font-family:Tahoma,"Segoe UI",sans-serif;margin:0;background:var(--bg);color:var(--text)}button,input,select{font:inherit}button{cursor:pointer}header{padding:22px max(20px,calc((100vw - 1360px)/2));display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);background:#101b2b;gap:16px;flex-wrap:wrap}.brand{font-size:23px;font-weight:800;letter-spacing:.2px}.brand span{color:var(--green)}.sub{font-size:12px;color:var(--muted);margin-top:7px}.wrap{max-width:1400px;margin:auto;padding:25px 20px 60px}.panel,.metric{background:var(--panel);border:1px solid var(--border);border-radius:14px}.metrics{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin-bottom:18px}.metric{padding:18px}.metric b{font-size:26px;display:block;direction:ltr;text-align:right;margin-top:9px}.metric small{color:var(--muted)}.section{padding:18px;margin-bottom:18px}.section-title{display:flex;justify-content:space-between;gap:15px;align-items:center;flex-wrap:wrap;margin-bottom:15px}.section-title h2{margin:0;font-size:18px}.muted{color:var(--muted);font-size:12px}.toolbar{display:flex;gap:10px;flex-wrap:wrap;align-items:end}.field{display:flex;flex-direction:column;gap:7px;min-width:135px}.field label{font-size:12px;color:var(--muted)}input,select{padding:11px 12px;border:1px solid var(--border);background:#0c1828;color:var(--text);border-radius:9px;outline:none;max-width:100%}input:focus,select:focus{border-color:var(--blue)}input[type=number]{width:120px}button,.button{border:1px solid var(--border);color:var(--text);border-radius:9px;background:var(--panel2);padding:10px 15px;text-decoration:none;white-space:nowrap}button:hover,.button:hover{border-color:var(--blue)}.primary{background:#147e65;border-color:#147e65;font-weight:700}#tvRun:disabled{opacity:.35;filter:grayscale(.8);cursor:not-allowed;pointer-events:none}.tabs{display:flex;gap:8px;margin-bottom:18px;flex-wrap:wrap}.tabs button.active{background:#284668;border-color:#5791d7}.table-scroll{overflow:auto;max-height:610px;border:1px solid var(--border);border-radius:10px}table{border-collapse:collapse;width:100%;font-size:12.5px;white-space:nowrap}th,td{padding:12px 13px;border-bottom:1px solid #263650;text-align:right}th{position:sticky;top:0;background:#1b2b42;color:#c7d9f3;z-index:2}tr:hover{background:#1c3049}.num{direction:ltr;text-align:right;font-variant-numeric:tabular-nums}.up{color:var(--green)}.tag{display:inline-block;border-radius:7px;padding:5px 8px;font-size:11px;font-weight:bold}.early{color:#59d5c4;background:#103d3c}.post{color:#ffd184;background:#483720}.actions{display:flex;align-items:center;gap:9px;justify-content:space-between;margin-top:12px;flex-wrap:wrap}.hidden{display:none!important}.years{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:15px 0}.year{border:1px solid var(--border);padding:15px;border-radius:10px;background:var(--panel2)}.year strong{font-size:22px}.year .line{display:flex;justify-content:space-between;margin-top:10px;color:var(--muted);font-size:12px}.monthbars{display:flex;gap:3px;align-items:end;height:112px;overflow:hidden}.bar{min-width:10px;flex:1;background:linear-gradient(0deg,#237a8a,#42d4a4);border-radius:3px 3px 0 0;cursor:help}pre{direction:ltr;text-align:left;white-space:pre-wrap;overflow-wrap:anywhere;max-height:260px;overflow:auto;background:#0b1523;border:1px solid var(--border);padding:12px;border-radius:9px;font-size:11px}.notice{padding:11px 14px;background:#202f40;color:#c4d9ee;border-radius:9px;margin-bottom:15px;font-size:12px;line-height:1.8}.ok{color:var(--green)}.bad{color:var(--red)}.small{font-size:12px}.loader{display:inline-block;width:12px;height:12px;border:2px solid #8ecbbb;border-top-color:transparent;border-radius:50%;animation:spin 1s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}@media(max-width:850px){.metrics{grid-template-columns:repeat(2,1fr)}.years{grid-template-columns:1fr}header{padding:16px}.wrap{padding:16px}}\nth.sortable{cursor:pointer;user-select:none;white-space:nowrap}th.sortable:hover{background:#28425e;color:#fff}th.sortable:focus-visible{outline:2px solid var(--blue);outline-offset:-2px}.sort-arrow{display:inline-block;min-width:13px;color:var(--green);font-weight:bold}\n\n/* CCAP: full page-height table, no nested scroll. */\nbody .wrap{max-width:none;width:100%;padding-inline:16px}\n#section-ccap .ccap-full-table{overflow:visible;max-height:none;width:100%;border-radius:10px}\n#section-ccap .ccap-full-table table{width:100%;min-width:0;table-layout:fixed;white-space:normal;font-size:clamp(9px,.72vw,12px)}\n#section-ccap .ccap-full-table th,#section-ccap .ccap-full-table td{padding:9px 3px;text-align:center;white-space:normal;overflow-wrap:anywhere;word-break:normal;line-height:1.45;vertical-align:middle}\n#section-ccap .ccap-full-table th{position:static;white-space:normal}\n#section-ccap .ccap-full-table th:nth-child(1),#section-ccap .ccap-full-table td:nth-child(1){width:8%}\n#section-ccap .ccap-full-table th:nth-child(2),#section-ccap .ccap-full-table td:nth-child(2){width:7%;font-weight:700}\n#section-ccap .ccap-full-table th:nth-child(6),#section-ccap .ccap-full-table td:nth-child(6){width:8%}\n#section-ccap .ccap-full-table th:nth-child(12),#section-ccap .ccap-full-table td:nth-child(12){width:9%;font-size:clamp(9px,.68vw,11px)}\n@media(max-width:900px){ #section-ccap .ccap-full-table table{font-size:9px}#section-ccap .ccap-full-table th,#section-ccap .ccap-full-table td{padding:7px 2px}}\n\n/* CCAP: tint same-day multi-signal groups; alternate tones across groups. */\n#section-ccap .ccap-full-table tbody tr.ccap-group-gold{background:#594519;color:#fff2cf}\n#section-ccap .ccap-full-table tbody tr.ccap-group-gold td{border-bottom-color:#7e652c}\n#section-ccap .ccap-full-table tbody tr.ccap-group-blue{background:#173f59;color:#d9f3ff}\n#section-ccap .ccap-full-table tbody tr.ccap-group-blue td{border-bottom-color:#32617a}\n#section-ccap .ccap-full-table tbody tr.ccap-group-gold:hover{background:#735a23}\n#section-ccap .ccap-full-table tbody tr.ccap-group-blue:hover{background:#215574}\n</style></head><body><header><div><div class="brand">نظام القوة النسبية لسوق المال المصري</div></div><div id="tvInfo" class="muted">جارٍ تحميل حالة البيانات...</div></header><div class="wrap"><div class="tabs"><button id="tab-ccap" class="active" onclick="showTab(\'ccap\')">نظام القوة النسبية لسوق المال المصري</button><button id="tvRun" class="primary" onclick="runUpdate()" disabled>↻ تحديث البيانات</button><span class="muted" id="tvLastDate">آخر تداول محفوظ: جارٍ الفحص...</span><span class="muted" id="tvStatus" aria-live="polite">جاهز</span></div><section class="panel section" aria-label="عن النظام" style="margin-bottom:18px"><div class="section-title"><h2>عن النظام</h2></div><div style="line-height:2;color:var(--text)"><div>نظام تحليلي متخصص في رصد فرص الأسهم بالبورصة المصرية باستخدام القوة النسبية وحركة الأسعار.</div><div>يعتمد على تحليل المراحل السعرية لاكتشاف التحولات المحتملة من الضعف إلى التجميع ثم الصعود.</div><div>يعرض فرص الشراء المحتملة وأداء الإشارات التاريخية خلال 52 أسبوعًا من ظهورها.</div></div></section><section class="panel section" aria-label="إعدادات المؤشرات"><div class="section-title"><h2>إعدادات المؤشرات</h2><span class="muted">الإعدادات تحفظ على المتصفح الحالي</span></div><div class="toolbar" style="align-items:end"><div class="field"><label for="settingRS">قيمة مؤشر القوة النسبية (RS)</label><input type="number" id="settingRS" min="20" max="150" step="1" value="29"></div><div class="field"><label for="settingFast">متوسط حسابي قصير المدى</label><input type="number" id="settingFast" min="3" max="30" step="1" value="18"></div><div class="field"><label for="settingSlow">متوسط حسابي طويل المدى</label><input type="number" id="settingSlow" min="20" max="150" step="1" value="29"></div><button onclick="applyIndicatorSettings()" class="primary">تطبيق الإعدادات</button><button onclick="restoreIndicatorDefaults()">استعادة الافتراضي</button><span id="settingsMessage" class="muted" aria-live="polite"></span></div><div class="muted" style="margin-top:10px">القيم الافتراضية: RS 29 / قصير 18 / طويل 29 · يجب أن يكون المتوسط القصير أقل من الطويل. تغيير الإعدادات يتطلب فحصًا جديدًا لنتائج مختلفة.</div></section><section id="section-ccap" class="panel section">\n<div class="section-title"><h2>نظام القوة النسبية لسوق المال المصري</h2><button onclick="downloadCcap()">تصدير CSV</button></div>\n\n<div class="toolbar" style="margin-bottom:12px"><div class="field"><label>من تاريخ</label><input type="date" id="ccapStart" value="2024-01-01"></div><div class="field"><label>إلى تاريخ</label><input type="date" id="ccapEnd" value="2026-10-27"></div><div class="field"><label>بحث السهم</label><input id="ccapSearch" placeholder="رمز السهم" oninput="loadCcap()"></div><button onclick="loadCcap()">عرض المحفوظة</button><button class="primary" id="ccapRunButton" onclick="runCcap()">تشغيل الفحص</button><span id="ccapStatus" class="muted">جاهز</span></div>\n<div id="ccapTotal" class="muted">—</div><div class="ccap-full-table"><table style="white-space:nowrap;font-size:11px"><thead><tr><th>التاريخ</th><th>السهم</th><th>سعر الشراء المقترح</th><th>مدة التداول الضعيف</th><th>جلسات التجميع</th><th>مدة النمط</th><th>القاع</th><th>أعلى سعر</th><th>القمة</th><th>صعود 52أ %</th><th>جلسات القمة</th><th>آخر سعر</th></tr></thead><tbody id="ccapBody"></tbody></table></div>\n</section>\n\n\n</div><script>const $=x=>document.getElementById(x);\nconst fmt=(n,d=2)=>Number(n).toLocaleString(\'en-US\',{maximumFractionDigits:d,minimumFractionDigits:d});\nconst safe=s=>String(s??\'\').replace(/[&<>"\']/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\'}[c]));\nasync function api(path,opts){let r=await fetch(path,{cache:"no-store",...(opts||{})});let j=await r.json();if(!r.ok)throw new Error(j.error||\'فشل تحميل البيانات\');return j;}\nfunction validRange(s,e){\n const valid=d=>/^\\d{4}-\\d{2}-\\d{2}$/.test(d)&&Number(d.slice(0,4))>=2019&&Number(d.slice(0,4))<=2100&&!isNaN(Date.parse(d));\n return valid(s)&&valid(e)&&s<=e;\n}\nfunction clearInvalidRange(prefix){\n const message=\'التاريخ غير صحيح: اكتبي السنة كاملة (مثال 2026)، وتأكدي أن البداية قبل النهاية.\';\n const total=$(prefix+\'Total\'),body=$(prefix+\'Body\');\n if(total)total.textContent=message;\n if(body)body.innerHTML=\'<tr><td colspan="\'+(prefix===\'ccap\'?10:13)+\'">\'+message+\'</td></tr>\';\n if(prefix===\'ames\')amesRows=[];else if(prefix===\'egts\')egtsRows=[];else ccapRows=[];\n}\n\nfunction dateRange(prefix){const start=$(prefix+\'Start\').value,end=$(prefix+\'End\').value;if(!validRange(start,end)){clearInvalidRange(prefix);throw new Error(\'سنة غير صحيحة أو بداية بعد النهاية\');}return {start,end};}\nlet selectedTab=\'ccap\';function showTab(){loadCcap();}\nlet tvAvailable=false,tvRunning=false,tvWasRunning=false;\nasync function refreshTvAvailability(){\n try{\n  const d=await api(\'/api/update/availability\');\n  $(\'tvLastDate\').textContent=\'آخر تداول محفوظ: \'+(d.stored||\'غير متاح\')+(d.available?\' | آخر جلسة متاحة: \'+d.available:\'\');\n  tvAvailable=Boolean(d.needs_update&&!d.checking&&!d.running);\n  tvRunning=Boolean(d.running);\n  $(\'tvRun\').disabled=!tvAvailable;\n  $(\'tvRun\').title=d.running?\'التحديث جارٍ\':d.checking?\'جارٍ التحقق من TradingView\':d.error?\'تعذر التحقق: \'+d.error:d.up_to_date?\'البيانات محدثة لآخر جلسة متاحة\':d.needs_update?\'توجد جلسة أحدث؛ اضغطي للتحديث\':\'انتظار تأكيد أحدث جلسة\';\n  if(!d.running){\n    if(d.checking){$(\'tvStatus\').textContent=\'جارٍ التحقق من أحدث جلسة تداول...\';}\n    else if(d.error){$(\'tvStatus\').textContent=\'تعذر التحقق من TradingView؛ \'+d.error;}\n    else if(d.up_to_date){$(\'tvStatus\').textContent=\'البيانات محدثة\';}\n    else if(d.needs_update){$(\'tvStatus\').textContent=\'توجد بيانات أحدث\';}\n  }\n }catch(e){tvAvailable=false;$(\'tvRun\').disabled=true;$(\'tvLastDate\').textContent=\'تعذر قراءة تاريخ التداول\';$(\'tvStatus\').textContent=e.message;}\n}\nasync function getTvInfo(){\n try{const d=await api(\'/api/data\');$(\'tvInfo\').textContent=\'عدد الأسهم: \'+d.symbols+\' | آخر جلسة: \'+d.latest+\' | المؤشرات: \'+Object.entries(d.indices).map(([k,v])=>k+\' \'+v).join(\' · \');}\n catch(e){$(\'tvInfo\').textContent=\'تعذر قراءة بيانات TradingView: \'+e.message;}\n await refreshTvAvailability();\n}\nasync function runUpdate(){\n if(!tvAvailable||tvRunning)return;\n $(\'tvRun\').disabled=true;\n try{await api(\'/api/update/run\',{method:\'POST\'});tvAvailable=false;tvRunning=true;$(\'tvStatus\').textContent=\'بدأ التحديث\';updatePoll();}\n catch(e){$(\'tvStatus\').textContent=e.message;refreshTvAvailability();}\n}\nlet updateFinished=false;\nasync function updatePoll(){\n try{\n  let s=await api(\'/api/update/status\');\n  tvRunning=Boolean(s.running);\n  $(\'tvRun\').disabled=tvRunning||!tvAvailable;\n  if(s.running){$(\'tvStatus\').textContent=\'جارٍ تحديث الأسعار...\';tvWasRunning=true;}\n  \n  if(!s.running&&tvWasRunning){\n   tvWasRunning=false;tvAvailable=false;$(\'tvRun\').disabled=true;\n   $(\'tvStatus\').textContent=s.message;\n   if(!s.ok)\n   // Force backend revalidation so the button changes state after a finished update.\n   getTvInfo();\n  }\n }catch(e){$(\'tvStatus\').textContent=e.message;}\n}\n\n// Indicator settings are isolated in this browser; no user-account authentication is implied.\nconst indicatorDefaults={rs:29,ma_fast:18,ma_slow:29};\nfunction indicatorSettings(){return {rs:Number($(\'settingRS\').value),ma_fast:Number($(\'settingFast\').value),ma_slow:Number($(\'settingSlow\').value)};}\nfunction validateIndicatorSettings(s){return Number.isInteger(s.rs)&&s.rs>=20&&s.rs<=150&&Number.isInteger(s.ma_fast)&&s.ma_fast>=3&&s.ma_fast<=30&&Number.isInteger(s.ma_slow)&&s.ma_slow>=20&&s.ma_slow<=150&&s.ma_fast<s.ma_slow;}\nfunction settingsQuery(){const s=indicatorSettings();if(!validateIndicatorSettings(s))throw new Error(\'الإعدادات غير صحيحة: المتوسط القصير يجب أن يكون أقل من الطويل، مع الالتزام بالحدود\');return s;}\nfunction applyIndicatorSettings(){try{const s=settingsQuery();localStorage.setItem(\'egx_indicator_settings_v2\',JSON.stringify(s));$(\'settingsMessage\').textContent=\'تم حفظ الإعدادات؛ شغّلي الفحص للحصول على نتائجها\';loadCcap();}catch(e){$(\'settingsMessage\').textContent=e.message;}}\nfunction restoreIndicatorDefaults(){$(\'settingRS\').value=29;$(\'settingFast\').value=18;$(\'settingSlow\').value=29;applyIndicatorSettings();}\nfunction initializeIndicatorSettings(){try{const saved=JSON.parse(localStorage.getItem(\'egx_indicator_settings_v2\')||\'null\');if(saved&&validateIndicatorSettings(saved)){$(\'settingRS\').value=saved.rs;$(\'settingFast\').value=saved.ma_fast;$(\'settingSlow\').value=saved.ma_slow;}}catch(e){}}\n// CCAP: date range and stock lookup only. No advanced filters or saved-filter widgets.\nfunction ccapParams(){return new URLSearchParams({start:$(\'ccapStart\').value,end:$(\'ccapEnd\').value,q:$(\'ccapSearch\').value.trim(),...settingsQuery()});}\nfunction downloadCcap(){if(!validRange($(\'ccapStart\').value,$(\'ccapEnd\').value)){clearInvalidRange(\'ccap\');return;}window.location.href=\'/download/ccap-signals.csv?\'+ccapParams().toString();}\nlet ccapRows=[],ccapSortKey=\'signal_date\',ccapSortDir=\'desc\',ccapLoadSeq=0,ccapFinished=false;\nconst ccapKeys=[\'signal_date\',\'symbol\',\'close\',\'purple_sessions\',\'gray_sessions\',\'sequence_sessions\',\'purple_min_rps\',\'peak_after\',\'peak_date\',\'peak_gain_pct\',\'sessions_to_peak\',\'latest_close\'];\nfunction sortCcap(k){if(ccapSortKey===k)ccapSortDir=ccapSortDir===\'asc\'?\'desc\':\'asc\';else{ccapSortKey=k;ccapSortDir=\'asc\';}renderCcap();}\nfunction renderCcap(){const nums=new Set([\'close\',\'purple_sessions\',\'gray_sessions\',\'sequence_sessions\',\'purple_min_rps\',\'peak_after\',\'peak_gain_pct\',\'sessions_to_peak\',\'latest_close\',\'current_return_pct\']);const rows=[...ccapRows].sort((a,b)=>{let x=a[ccapSortKey],y=b[ccapSortKey];let d=nums.has(ccapSortKey)?Number(x||0)-Number(y||0):String(x||\'\').localeCompare(String(y||\'\'));return ccapSortDir===\'asc\'?d:-d;});\n// Color only dates with 2+ signals among the currently displayed results.\n// Alternate group colors in current sorted order, including when groups touch.\nconst dateCounts=new Map();for(const x of rows){const d=String(x.signal_date||\'\');dateCounts.set(d,(dateCounts.get(d)||0)+1);}\nconst groupedDays=[];for(const x of rows){const d=String(x.signal_date||\'\');if(dateCounts.get(d)>1&&!groupedDays.includes(d))groupedDays.push(d);}\nconst dateColors=new Map(groupedDays.map((d,i)=>[d,i%2===0?\'ccap-group-gold\':\'ccap-group-blue\']));\n$(\'ccapBody\').innerHTML=rows.map(x=>`<tr class="${dateColors.get(String(x.signal_date||\'\'))||\'\'}"><td>${safe(x.signal_date)}</td><td><b>${safe(x.symbol)}</b></td><td>${fmt(x.close,4)}</td><td>${safe(x.purple_sessions)}</td><td>${safe(x.gray_sessions)}</td><td>${safe(x.sequence_sessions)}</td><td>${fmt(x.purple_min_rps,4)}</td><td>${x.peak_after?fmt(x.peak_after,4):\'—\'}</td><td>${safe(x.peak_date||\'—\')}</td><td>${x.peak_gain_pct!==\'\'&&x.peak_gain_pct!=null?fmt(x.peak_gain_pct,2)+\'%\':\'—\'}</td><td>${safe(x.sessions_to_peak||\'—\')}</td><td>${x.latest_close?fmt(x.latest_close,4):\'—\'}</td></tr>`).join(\'\')||\'<tr><td colspan="12">لا توجد نتائج مطابقة</td></tr>\';\ndocument.querySelectorAll(\'#section-ccap thead th\').forEach((th,i)=>{th.style.cursor=\'pointer\';th.onclick=()=>sortCcap(ccapKeys[i]);});}\nasync function loadCcap(){const seq=++ccapLoadSeq;try{if(!validRange($(\'ccapStart\').value,$(\'ccapEnd\').value)){clearInvalidRange(\'ccap\');return;}ccapRows=[];renderCcap();$(\'ccapTotal\').textContent=\'جارٍ التحميل...\';const d=await api(\'/api/ccap/signals?\'+ccapParams().toString());if(seq!==ccapLoadSeq)return;ccapRows=d.rows||[];$(\'ccapTotal\').textContent=ccapRows.length+\' إشارة\'+(d.saved_exists?\'\':\' — لم يُشغل الفحص بعد\');renderCcap();}catch(e){if(seq!==ccapLoadSeq)return;ccapRows=[];renderCcap();$(\'ccapTotal\').textContent=\'خطأ: \'+e.message;}}\nasync function runCcap(){try{await api(\'/api/ccap/run\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(settingsQuery())});$(\'ccapStatus\').textContent=\'بدأ الفحص\';ccapFinished=false;ccapPoll();}catch(e){$(\'ccapStatus\').textContent=e.message;}}\nasync function ccapPoll(){try{const s=await api(\'/api/ccap/status?\'+new URLSearchParams(settingsQuery()).toString());$(\'ccapRunButton\').disabled=s.running;$(\'ccapStatus\').textContent=s.running?\'جارٍ الفحص...\':s.message;if(!s.running&&s.ok&&!ccapFinished){ccapFinished=true;loadCcap();}if(s.running)ccapFinished=false;}catch(e){$(\'ccapStatus\').textContent=e.message;}}\n\ninitializeIndicatorSettings();showTab(\'ccap\');getTvInfo();ccapPoll();updatePoll();setInterval(ccapPoll,2500);setInterval(updatePoll,2500);setInterval(refreshTvAvailability,60000);\n</script></body></html>'


if __name__ == "__main__":
    app.run(host=os.environ.get('HOST','0.0.0.0'), port=int(os.environ.get('PORT','8765')), debug=False, use_reloader=False)
