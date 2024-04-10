# -*- coding: utf-8 -*-
"""
Created on Tue May 18 00:53:21 2021

@author: tomni
"""
from binance.client import Client
from binance.exceptions import BinanceAPIException
import configparser
import time
import numpy as np
import pandas as pd
import logging
import logging.handlers as handlers
import os
import schedule

config = configparser.ConfigParser()
config.read_file(open('keys.cfg'))
api_key = config.get('BINANCE', 'ACTUAL_API_KEY')
secret_key = config.get('BINANCE', 'ACTUAL_SECRET_KEY')
client = Client(api_key, secret_key)

desired_balance = {
    "BTC": 0.15,
    "ETH": 0.15,
    "ADA": 0.125,
    "DOT": 0.125,
    "VET": 0.125,
    "LINK": 0.125,
    "BNB": 0.10,
    "ONE": 0.10,
}
threshold = 0.05

class portefolio:
    def get_assets(self):
        #print('get_assets:',time.strftime("%H:%M:%S", time.localtime()))
        global assets, balances, token_pairs
        assets = []
        balances = []
        token_pairs = ['BTCBUSD','BTCEUR']
        info = client.get_account()
        for index in range(len(info['balances'])):
            if float(info['balances'][index]['free']) > 0 and info['balances'][index]['asset'] not in ['BUSD','USDT','EUR']:
                for key in info['balances'][index]:
                    if key == 'asset':
                        assets.append(info['balances'][index][key])
                    if key == 'free':
                        balances.append(float(info['balances'][index][key]))
        for token in assets:
            if token != 'BTC':
                token_pairs.append(token + 'BTC')
        for token in desired_balance.keys():
            if token != 'BTC' and (token+'BTC') not in token_pairs:
                token_pairs.append(token + 'BTC')
        return
    
    def ticker_price(self,symbol):
        price = client.get_symbol_ticker(symbol=symbol)['price']
        return price
    
    def get_exchange_btc(self):
        global token_btc
        token_btc = {}
        for tokenpair in token_pairs:
            price = self.ticker_price(tokenpair)
            token_btc[tokenpair] = float(price)
        return token_btc
    
    def assets_btc(self):
        global balances_btc
        balances_btc = []
        for i, asset in enumerate(assets):
            if asset == 'BTC':
                balances_btc.append(balances[i])
            else:
                balances_btc.append(balances[i] * token_btc[asset+'BTC'])
        return balances_btc
    
    def assets_per(self):
        global assets_percentage
        assets_percentage = []
        sum_btc = sum(self.assets_btc())
        for i,asset in enumerate(assets):
            assets_percentage.append(self.assets_btc()[i]/sum_btc)
        return assets_percentage
    
    def btc_busd(self,val_btc):
        val_busd = val_btc*token_btc['BTCBUSD']
        return val_busd
        
    def btc_eur(self,val_btc):
        val_eur = val_btc*token_btc['BTCEUR']
        return val_eur
    
    def calc_deviation(self):
        global deviation
        deviation =[]
        for i, asset in enumerate(assets):
            if asset in desired_balance:
                deviation.append((self.assets_per()[i] - desired_balance[asset])/desired_balance[asset])
            else:
                deviation.append(1000)
        return deviation
    
    def make_dataframe(self):
        global df
        df = pd.DataFrame.from_dict(desired_balance,orient='index',columns=['desired_percentage'])
        for i, asset in enumerate(assets):
            if asset not in df.index:
                df = df.append(pd.DataFrame([0],columns=['desired_percentage'],index=[asset]))
        df = df.assign(current_percentage=np.zeros(len(df.index)))
        df = df.assign(deviation=-1000*np.ones(len(df.index)))
        df = df.assign(current_balance=np.zeros(len(df.index)))
        df = df.assign(balance_btc=np.zeros(len(df.index)))
        df = df.assign(balance_busd=np.zeros(len(df.index)))
        df = df.assign(balance_eur=np.zeros(len(df.index)))
        df.loc['Total']= df.sum(numeric_only=True, axis=0)
        return df
    
    def update_dataframe(self):
        self.get_exchange_btc()
        self.calc_deviation()
        for i, asset in enumerate(assets):
            df.at[asset,'current_percentage'] = assets_percentage[i]
            df.at[asset,'deviation'] = deviation[i]
            df.at[asset,'current_balance'] = balances[i]
            df.at[asset,'balance_btc'] = balances_btc[i]
            df.at[asset,'balance_busd'] = self.btc_busd(balances_btc[i])
            df.at[asset,'balance_eur'] = self.btc_eur(balances_btc[i])
        df.loc['Total']= df[:-1].sum(numeric_only=True)
        #with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
        print(df['deviation'],'\nTotal eur:',float(df.at['Total','balance_eur']))
        return df


class rebalance:
    def check_balance(self,df):
        #print('check_balance:',time.strftime("%H:%M:%S", time.localtime()))
        pf.update_dataframe()
        for coin in df.index:
            deviation = float(df.at[coin,'deviation'])
            if coin not in ('Total','BTC'):
                if abs(deviation) <= threshold:
                    continue
                elif 1000 > deviation > threshold:
                    percentage = deviation/(1+deviation)
                    amount = percentage*df.at[coin,'current_balance']
                    buyorsell = "SELL"
                elif -1000 < deviation < -threshold:
                    percentage = abs(deviation/(1+deviation))
                    amount = percentage*df.at[coin,'current_balance']
                    buyorsell = "BUY"
                elif deviation >= 1000:
                    amount = df.at[coin,'current_balance']
                    buyorsell = "SELL"
                elif deviation <= -1000:
                    btc_balance = df.at['Total','balance_btc']*df.at[coin,'desired_percentage']
                    amount = btc_balance/token_btc[coin+'BTC']
                    buyorsell = "BUY"   
                else:
                    continue
                logger.info('threshold triggered: %s %s',deviation,coin)
                self.create_market_order(coin+'BTC',amount,buyorsell)
            else:
                continue
        return 
    
    def check_order(self,pair,amount,buyorsell):
        notional = float(amount*token_btc[pair])
        if notional > float(client.get_symbol_info(pair)['filters'][3]['minNotional']):
            try:
                test_order = client.create_test_order(
                    symbol=pair,
                    side=buyorsell,
                    type="MARKET",
                    quantity=amount,)
                time.sleep(1)
            except BinanceAPIException as e:
                logger.error('Test order failed: %s %s %s %s',buyorsell,amount,pair,e)
                return False
            else:
                logger.info('Test order succesful: %s %s %s %s',buyorsell,amount,pair,test_order)
                return True
        else:
            logger.warning('notional too small: %s %s %s',pair,notional,str(client.get_symbol_info(pair)['filters'][3]['minNotional']))
            return False

    def create_market_order(self,pair,amount,buyorsell):
        ticks = float(client.get_symbol_info(pair)['filters'][2]['stepSize'])
        amount = round(amount,-int(np.log10(ticks)))
        if self.check_order(pair,amount,buyorsell) == True:
            try:
                order = client.create_order(
                    symbol=pair,
                    side=buyorsell,
                    type="MARKET",
                    quantity=amount,)
                time.sleep(1)
            except BinanceAPIException as e:
                logger.error('order failed: %s %s %s %s',buyorsell,amount,pair,e)
                return False
            else:
                pf.get_assets()
                logger.info('order succesful: %s',order)
                return True
            return order
        else:
            return False

def set_logger():
    try:
        os.mkdir('logs')
    except FileExistsError:
        pass
    logger = logging.getLogger('balance')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    timehandler = handlers.TimedRotatingFileHandler('logs/balance.log', when="midnight", interval=1)
    timehandler.setLevel(logging.INFO)
    timehandler.suffix = "%Y%m%d"
    timehandler.setFormatter(formatter)
    logger.addHandler(timehandler)
    return logger

def main():
    pf.get_assets()
    pf.get_exchange_btc()
    pf.make_dataframe()
    
    schedule.every(10).seconds.do(rb.check_balance, df=df)
    schedule.every().minute.at(":00").do(pf.get_assets)
    while True:
        schedule.run_pending()
        time.sleep(1)

logger = set_logger()
pf = portefolio()
rb = rebalance()
while True:
    try:
        main()
    except KeyboardInterrupt:
        schedule.clear()
        break
    except Exception as e:
        logger.critical(e)
        schedule.clear()
        print(e)
        
