# -*- coding: utf-8 -*-
"""CCAP Purple -> Gray -> Green signal search.

Exact consecutive COLOR RUNS: IUP -> RUP -> LOP. The IUP run must contain
at least one raw RPS reading strictly less than -0.21. Buy date is the first
LOP day immediately following RUP. No price filter or extra RPS threshold.
Uses read-only adjusted TradingView stages and EGX30. Existing CCAP V5
artifacts and all other systems are left untouched.
"""
import argparse
import os
from datetime import timedelta
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from egx_rps_three_patterns import compute

FIELDS = ['symbol','signal_date','close','rps_at_buy','gray_at_buy',
          'purple_start','purple_end','purple_sessions','purple_min_rps',
          'purple_min_date','gray_start','gray_end','gray_sessions',
          'green_start','sequence_sessions','rps_threshold','peak_after','peak_date','peak_gain_pct','sessions_to_peak','latest_close','latest_date','current_return_pct']

def scan(x,symbol,threshold=-0.21):
    x=x.reset_index(drop=True)
    codes=x.pine_color_code.tolist()
    runs=[]
    for i,c in enumerate(codes):
        if not runs or runs[-1][0]!=c:
            runs.append([c,i,i])
        else:
            runs[-1][2]=i
    found=[]
    for j in range(2,len(runs)):
        purple,gray,green = runs[j-2:j+1]
        if [purple[0],gray[0],green[0]]!=['IUP','RUP','LOP']:
            continue
        p=x.iloc[purple[1]:purple[2]+1]
        eligible=p[(p.rps.notna())&(p.rps<threshold)]
        if eligible.empty:
            continue
        low_index=eligible.rps.idxmin()
        g=x.iloc[green[1]]
        found.append(dict(symbol=symbol,signal_date=str(g.date),close=round(float(g.c),6),
           rps_at_buy=round(float(g.rps),8),gray_at_buy=round(float(g.gray_sma50),8),
           purple_start=str(x.date.iloc[purple[1]]),purple_end=str(x.date.iloc[purple[2]]),
           purple_sessions=purple[2]-purple[1]+1,purple_min_rps=round(float(x.rps.iloc[low_index]),8),
           purple_min_date=str(x.date.iloc[low_index]),
           gray_start=str(x.date.iloc[gray[1]]),gray_end=str(x.date.iloc[gray[2]]),
           gray_sessions=gray[2]-gray[1]+1,green_start=str(g.date),
           sequence_sessions=green[1]-purple[1]+1,rps_threshold=threshold))
    return found

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--start',default='2019-01-01')
    p.add_argument('--end',default='2099-12-31')
    p.add_argument('--threshold',type=float,default=-0.21)
    p.add_argument('--rs',type=int,default=29)
    p.add_argument('--ma-fast',type=int,default=18)
    p.add_argument('--ma-slow',type=int,default=29)
    p.add_argument('--output',default='EGX_CCAP_PURPLE_GRAY_GREEN')
    a=p.parse_args()
    if a.start>a.end:p.error('start date after end date')
    if not (20 <= a.rs <= 150 and 3 <= a.ma_fast <= 30 and 20 <= a.ma_slow <= 150 and a.ma_fast < a.ma_slow):
        p.error('Invalid periods: RS 20..150, MA short 3..30, MA long 20..150, short < long')
    if not os.getenv('DATABASE_URL'):p.error('DATABASE_URL missing')
    eng=create_engine(os.environ['DATABASE_URL'],pool_pre_ping=True,
                      connect_args={'options':'-c default_transaction_read_only=on'})
    try:
        with eng.connect() as con:
            # Railway commonly names its PostgreSQL database 'railway'.
            # Verify required table schemas instead of assuming a database name.
            st=pd.read_sql(text('SELECT symbol,session_date,h,c FROM public.eg_tv_stocks_1d ORDER BY symbol,session_date'),con)
            ix=pd.read_sql(text("SELECT session_date,c AS bc FROM public.eg_tv_indices_1d WHERE symbol='EGX30' ORDER BY session_date"),con)
    finally:eng.dispose()
    if st.empty or ix.empty:raise SystemExit('Missing TradingView stock or EGX30 rows')
    ix.session_date=pd.to_datetime(ix.session_date).dt.normalize()
    ix.bc=pd.to_numeric(ix.bc,errors='coerce')
    if ix.session_date.duplicated().any() or ix.bc.isna().any() or ix.bc.le(0).any():
        raise SystemExit('Invalid benchmark history')
    st.session_date=pd.to_datetime(st.session_date).dt.normalize()
    st.c=pd.to_numeric(st.c,errors='coerce')
    st.h=pd.to_numeric(st.h,errors='coerce')
    allrows=[];errors=[];checked=0
    print('START Purple(IUP RPS < %s) -> Gray(RUP) -> Green(LOP)' % a.threshold,flush=True)
    for symbol,frame in st.groupby('symbol',sort=True):
        try:
            if frame.session_date.duplicated().any() or frame.c.isna().any() or frame.c.le(0).any():
                raise ValueError('Invalid or duplicate stock sessions')
            x=compute(frame[['session_date','c']],ix[['session_date','bc']],a.rs,a.ma_fast,a.ma_slow)
            hits=[r for r in scan(x,symbol,a.threshold) if a.start<=r['signal_date']<=a.end]
            bars=frame.sort_values('session_date').reset_index(drop=True)
            dates=bars.session_date.dt.strftime('%Y-%m-%d').tolist()
            from bisect import bisect_right
            valid_closes=bars[ bars.c.notna() & bars.c.gt(0) ]
            last=valid_closes.iloc[-1] if not valid_closes.empty else None
            for r in hits:
                # Peak window: sessions strictly AFTER signal, through 52 calendar weeks (364 days).
                # Only actual observed TradingView bars; never forward-fill or synthesize sessions.
                end52=(pd.Timestamp(r['signal_date'])+timedelta(weeks=52)).strftime('%Y-%m-%d')
                after=bars.iloc[bisect_right(dates,r['signal_date']):bisect_right(dates,end52)]
                possible=after[after.h.notna() & after.h.gt(0)]
                r.update(peak_after='',peak_date='',peak_gain_pct='',sessions_to_peak='',
                         latest_close='',latest_date='',current_return_pct='')
                if not possible.empty:
                    pos=possible.h.idxmax();peak=bars.loc[pos]
                    r['peak_after']=round(float(peak.h),6)
                    r['peak_date']=peak.session_date.strftime('%Y-%m-%d')
                    r['peak_gain_pct']=round((float(peak.h)/r['close']-1)*100,4)
                    r['sessions_to_peak']=int((after.index<=pos).sum())
                if last is not None:
                    r['latest_close']=round(float(last.c),6)
                    r['latest_date']=last.session_date.strftime('%Y-%m-%d')
                    r['current_return_pct']=round((float(last.c)/r['close']-1)*100,4)
            allrows.extend(hits)
            checked+=1
            if checked%25==0:print('PROGRESS',checked,'signals',len(allrows),'errors',len(errors),flush=True)
        except Exception as exc:
            errors.append({'symbol':symbol,'error':str(exc)})
    allrows.sort(key=lambda r:(r['signal_date'],r['symbol']))
    dest=Path(a.output);dest.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(allrows,columns=FIELDS).to_csv(dest/'ALL_MARKET_SIGNALS.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(errors,columns=['symbol','error']).to_csv(dest/'ERRORS.csv',index=False,encoding='utf-8-sig')
    ref=[r for r in allrows if r['symbol']=='CCAP.CA' and r['signal_date']=='2025-11-26']
    pd.DataFrame(ref,columns=FIELDS).to_csv(dest/'CCAP_REFERENCE_CHECK.csv',index=False,encoding='utf-8-sig')
    print('FINISHED checked',checked,'signals',len(allrows),'errors',len(errors),flush=True)
    print('REFERENCE CCAP.CA 2025-11-26', 'MATCH' if ref else 'NOT MATCHED',flush=True)
    print('SAVED',dest.resolve(),'READ ONLY',flush=True)
if __name__=='__main__':main()
