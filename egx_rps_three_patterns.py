# -*- coding: utf-8 -*-
"""TradingView RPS: extract color state sequences around RPS/SMA50 crosses.
Three named example stocks only. Read-only, no missing-bar filling.
Uses original Pine's RP color rules and its independent grey SMA50.
Outputs full histories, each upward cross + neighborhood, and compact sequences.
"""
import argparse
import os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

STOCKS = ('EGTS.CA', 'CCAP.CA', 'AMES.CA')
LABELS = {
    'ROP':'GRAY (resilient outperformance)',
    'IOP':'BLUE (independent outperformance)',
    'LOP':'GREEN (leading outperformance)',
    'LAOP':'YELLOW (lagging outperformance)',
    'RUP':'DARKGRAY (resilient underperformance)',
    'IUP':'PURPLE (independent underperformance)',
    'LUP':'RED (leading underperformance)',
    'LAUP':'YELLOW (lagging underperformance)',
}

def compute(stock, index, lookback=50, smooth=7, gray=50):
    x = stock.merge(index, on='session_date', how='inner', validate='one_to_one').sort_values('session_date').reset_index(drop=True)
    if x.empty: return x
    s=x['c'].astype(float); b=x['bc'].astype(float)
    # Pine: first smooth current AND lagged input price by SMA7.
    ss=s.rolling(smooth,min_periods=smooth).mean()
    bb=b.rolling(smooth,min_periods=smooth).mean()
    x['rps']=ss/ss.shift(lookback) - bb/bb.shift(lookback)
    x['gray_sma50']=x.rps.rolling(gray,min_periods=gray).mean()
    x['above_gray']=(x.rps > x.gray_sma50)
    x['cross_up']=(x.rps > x.gray_sma50) & (x.rps.shift(1)<=x.gray_sma50.shift(1))
    x['cross_down']=(x.rps < x.gray_sma50) & (x.rps.shift(1)>=x.gray_sma50.shift(1))
    # Original Pine: AssetBull and CompareBull are 50-session performance signs,
    # computed on SMA7-smoothed prices with no log returns and no ATR normalization.
    asset_bull=ss/ss.shift(lookback)>1
    compare_bull=bb/bb.shift(lookback)>1
    outperform=x.rps>0
    underperform=x.rps<0
    # Preserve all eight Pine states. Condition order matches original Pine exactly.
    states=np.select([
      (~asset_bull)&(~compare_bull)&outperform,
      asset_bull&(~compare_bull)&outperform,
      asset_bull&compare_bull&outperform,
      (~asset_bull)&compare_bull&outperform,
      asset_bull&compare_bull&underperform,
      (~asset_bull)&compare_bull&underperform,
      (~asset_bull)&(~compare_bull)&underperform,
      asset_bull&(~compare_bull)&underperform,
    ], ['ROP','IOP','LOP','LAOP','RUP','IUP','LUP','LAUP'],default='NEUTRAL')
    x['pine_color_code']=states
    x.loc[x.rps.isna(),'pine_color_code']='WARMUP'
    x['rps_minus_gray']=x.rps-x.gray_sma50
    x['date']=x.session_date.dt.strftime('%Y-%m-%d')
    return x

def compress(seq):
    out=[]
    for color in seq:
        if color in ('WARMUP','NEUTRAL'):continue
        if not out or out[-1][0]!=color:out.append([color,1])
        else:out[-1][1]+=1
    return ' > '.join(f'{c}({n})' for c,n in out)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--start',default='2025-01-01')
    p.add_argument('--end',default='2026-10-24')
    p.add_argument('--period',type=int,default=50)
    p.add_argument('--price-sma',type=int,default=7)
    p.add_argument('--gray-sma',type=int,default=50)
    p.add_argument('--before',type=int,default=30)
    p.add_argument('--after',type=int,default=12)
    p.add_argument('--index',choices=['EGX30','EGX70EWI','EGX100EWI'],default='EGX30')
    p.add_argument('--output',default='EGX_RPS_THREE_PATTERNS')
    a=p.parse_args()
    if min(a.period,a.price_sma,a.gray_sma,a.before,a.after)<1:p.error('all periods must be >=1')
    if not os.getenv('DATABASE_URL'):p.error('Set DATABASE_URL')
    out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    eng=create_engine(os.environ['DATABASE_URL'],pool_pre_ping=True,connect_args={'options':'-c default_transaction_read_only=on'})
    summaries=[];windows=[]
    try:
        with eng.connect() as con:
            db=con.execute(text('select current_database()')).scalar()
            if db!='stocks_local':raise SystemExit('STOP: not stocks_local')
            idx=pd.read_sql(text('select session_date,c as bc from public.eg_tv_indices_1d where symbol=:s order by session_date'),con,params={'s':a.index})
            if idx.empty:raise SystemExit('No EGX30 index candles')
            idx.session_date=pd.to_datetime(idx.session_date).dt.normalize()
            if idx.session_date.duplicated().any() or (pd.to_numeric(idx.bc,errors='coerce')<=0).any():raise SystemExit('Invalid index data')
            for sym in STOCKS:
                stock=pd.read_sql(text('select session_date,c from public.eg_tv_stocks_1d where symbol=:s order by session_date'),con,params={'s':sym})
                if stock.empty:raise SystemExit('No stock data '+sym)
                stock.session_date=pd.to_datetime(stock.session_date).dt.normalize()
                if stock.session_date.duplicated().any() or (pd.to_numeric(stock.c,errors='coerce')<=0).any():raise SystemExit('Invalid stock '+sym)
                x=compute(stock,idx,a.period,a.price_sma,a.gray_sma)
                x.to_csv(out/f'{sym.split(".")[0]}_FULL_COLORS.csv',index=False,encoding='utf-8-sig')
                filt=(x.session_date>=pd.Timestamp(a.start))&(x.session_date<=pd.Timestamp(a.end))&x.cross_up
                cross_idx=x.index[filt].tolist()
                print(sym,'UP_CROSSES',len(cross_idx),'COLORS',x.pine_color_code.value_counts().to_dict(),flush=True)
                for k in cross_idx:
                    left=max(0,k-a.before);right=min(len(x),k+a.after+1)
                    w=x.iloc[left:right].copy()
                    before=x.iloc[left:k+1]
                    after=x.iloc[k:right]
                    # Capture exact colored sequence and outcome without imposing green final state.
                    record={'symbol':sym,'cross_date':x.date.iloc[k], 'rps':round(float(x.rps.iloc[k]),6),
                        'sma50':round(float(x.gray_sma50.iloc[k]),6), 'color_at_cross':x.pine_color_code.iloc[k],
                        'color_pre_cross':x.pine_color_code.iloc[k-1] if k>0 else '',
                        'color_after_5':x.pine_color_code.iloc[min(k+5,len(x)-1)],
                        'color_after_10':x.pine_color_code.iloc[min(k+10,len(x)-1)],
                        'before_colors':compress(before.pine_color_code.tolist()),
                        'after_colors':compress(after.pine_color_code.tolist()),
                        'full_color_sequence':compress(w.pine_color_code.tolist()),
                        'return_10_sessions_pct':round((float(x.c.iloc[k+10])/float(x.c.iloc[k])-1)*100,3) if k+10<len(x) else None}
                    summaries.append(record)
                    w.insert(0,'reference_stock',sym)
                    w.insert(1,'cross_date',x.date.iloc[k])
                    windows.append(w)
    finally:eng.dispose()
    s=pd.DataFrame(summaries)
    s.to_csv(out/'THREE_STOCK_CROSS_SEQUENCES.csv',index=False,encoding='utf-8-sig')
    if windows:pd.concat(windows,ignore_index=True).to_csv(out/'ALL_CROSS_WINDOWS.csv',index=False,encoding='utf-8-sig')
    for sym in STOCKS:
        matches=s[s.symbol.eq(sym)] if not s.empty else pd.DataFrame()
        if matches.empty: continue
        matches.to_csv(out/f'{sym.split(".")[0]}_PATTERN_CANDIDATES.csv',index=False,encoding='utf-8-sig')
        print('\n',sym,'CROSS DATES / PRE-CROSS COLORS / FINAL COLOR:',flush=True)
        print(matches[['cross_date','color_pre_cross','color_at_cross','color_after_10','before_colors']].tail(15).to_string(index=False,max_colwidth=86),flush=True)
    (out/'README.txt').write_text('These are EXACT Pine color classifications based on provided settings, using SMA7 on both stock and benchmark BEFORE RPS50, plus separate SMA50 on RPS. No missing data filled. Cross windows are diagnostic candidates, NOT validated to correspond to screenshot drawings. Compare the three separate outputs to the visible colored segments before naming/fixing three pattern rules. No DB writes.\n',encoding='utf-8')
    print('\nDONE',len(summaries),'CROSS WINDOWS',out.resolve(),flush=True)
    print('READ ONLY. No color finale assumed. Need chart confirmation before freezing patterns.',flush=True)
if __name__=='__main__':main()
