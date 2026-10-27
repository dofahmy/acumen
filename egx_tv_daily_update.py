# -*- coding: utf-8 -*-
"""Daily incremental TradingView EGX update. Appends NEW sessions only.
Uses existing TradingView staging as universe, not legacy price tables.
Unofficial tvDatafeed transport; no guarantee of availability or adjusted-history continuity.
"""
import argparse, os, sys, time
from datetime import date
from pathlib import Path
from sqlalchemy import create_engine, text, MetaData, Table, select, func
from sqlalchemy.dialects.postgresql import insert

INDEXES=('EGX30','EGX70EWI','EGX100EWI')
REQUIRED=('symbol','session_date','o','h','l','c','v')

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--bars',type=int,default=80)
    parser.add_argument('--delay',type=float,default=0.8)
    parser.add_argument('--max-errors',type=int,default=50)
    a=parser.parse_args()
    if not 10<=a.bars<=5000 or a.delay<0:parser.error('Invalid bars/delay')
    url=os.getenv('DATABASE_URL')
    if not url:sys.exit('ERROR DATABASE_URL missing')
    try:
        from tvDatafeed import TvDatafeed,Interval
    except ImportError:sys.exit('ERROR install tvdatafeed in same Python environment')
    engine=create_engine(url,pool_pre_ping=True)
    with engine.connect() as conn:
        # Do not require a specific database name on Railway; validate tables below.
        stocks=Table('eg_tv_stocks_1d',MetaData(),schema='public',autoload_with=conn)
        inds=Table('eg_tv_indices_1d',MetaData(),schema='public',autoload_with=conn)
        for tbl in (stocks,inds):
            missing=set(REQUIRED)-set(tbl.columns.keys())
            if missing:sys.exit('ERROR schema missing columns in '+tbl.name+': '+str(sorted(missing)))
        universe=conn.execute(select(stocks.c.symbol).distinct().order_by(stocks.c.symbol)).scalars().all()
        if not universe:sys.exit('ERROR TradingView stock universe empty')
        latest_stock={s:str(d)[:10] for s,d in conn.execute(text('SELECT symbol,MAX(session_date) FROM public.eg_tv_stocks_1d GROUP BY symbol'))}
        latest_index={s:str(d)[:10] for s,d in conn.execute(text('SELECT symbol,MAX(session_date) FROM public.eg_tv_indices_1d GROUP BY symbol'))}
    print('START TradingView daily update; symbols:',len(universe),'indices:',len(INDEXES),flush=True)
    print('SAFETY: TradingView staging only; append new sessions, never modify/delete existing candles',flush=True)
    print('NOTE: Unofficial tvDatafeed may fail or return limited history; old rows are kept unchanged',flush=True)
    tv=TvDatafeed()
    errors=[];added_stocks=0;added_indices=0;successful=0
    def fetch(symbol,kind,table,previous):
        nonlocal successful
        ticker=symbol[:-3] if symbol.upper().endswith('.CA') else symbol
        frame=tv.get_hist(symbol=ticker,exchange='EGX',interval=Interval.in_daily,n_bars=a.bars)
        if frame is None or frame.empty:raise RuntimeError('NO_TRADINGVIEW_BARS')
        frame=frame.reset_index()
        if 'datetime' not in frame.columns:raise RuntimeError('missing datetime')
        needed=('open','high','low','close','volume')
        if any(c not in frame for c in needed):raise RuntimeError('missing OHLCV')
        import pandas as pd
        frame['session_date']=pd.to_datetime(frame['datetime'],errors='coerce').dt.date
        if frame.session_date.isna().any():raise RuntimeError('invalid dates')
        if frame.session_date.duplicated().any():raise RuntimeError('duplicate TradingView dates')
        frame=frame.sort_values('session_date')
        if previous and frame.session_date.max().isoformat()<previous:
            raise RuntimeError('fetched history ends before stored latest session')
        rows=[]
        for r in frame.itertuples(index=False):
            day=r.session_date
            if previous and day.isoformat()<=previous:continue
            vals=[float(r.open),float(r.high),float(r.low),float(r.close),float(r.volume)]
            if any(not __import__('math').isfinite(v) for v in vals) or min(vals[:4])<=0 or vals[4]<0:
                raise RuntimeError('invalid price/volume')
            rows.append(dict(symbol=symbol,session_date=day,o=vals[0],h=vals[1],l=vals[2],c=vals[3],v=vals[4]))
        # Do not add partial-session or future bars. Fetch should be run after EGX close.
        rows=[r for r in rows if r['session_date']<=date.today()]
        if not rows:
            successful+=1
            return 0,'up to date'
        # Under concurrent execution, only duplicate (symbol,date) conflicts are ignored.
        # No historical price is overwritten; if old history is re-adjusted, manual audit is required.
        with engine.begin() as conn:
            stmt=insert(table).values(rows).on_conflict_do_nothing(index_elements=['symbol','session_date'])
            result=conn.execute(stmt)
            n=result.rowcount
        successful+=1
        return n,'last '+str(rows[-1]['session_date'])
    try:
        for kind,items,table,last in [('STOCK',universe,stocks,latest_stock),('INDEX',INDEXES,inds,latest_index)]:
            for i,sym in enumerate(items,1):
                try:
                    count,note=fetch(sym,kind,table,last.get(sym))
                    if kind=='STOCK':added_stocks+=count
                    else:added_indices+=count
                    print(f'[{kind} {i}/{len(items)}] {sym}: +{count} {note}',flush=True)
                except Exception as exc:
                    msg=f'{kind} {sym}: {type(exc).__name__}: {exc}'
                    errors.append(msg);print('ERROR',msg,flush=True)
                if a.delay:time.sleep(a.delay)
    finally:
        engine.dispose()
    print(f'FINISHED successful={successful} added_stock_rows={added_stocks} added_index_rows={added_indices} errors={len(errors)}',flush=True)
    if errors:
        for msg in errors:print('FAILED',msg,flush=True)
        sys.exit(2)
    print('SUCCESS. Source is unofficial TradingView feed. Historical adjusted-price audit recommended.',flush=True)

if __name__=='__main__':main()
