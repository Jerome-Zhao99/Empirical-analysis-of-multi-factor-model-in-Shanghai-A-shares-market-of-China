from CAL.PyCAL import *
import numpy as np
from pandas import DataFrame
start = '2020-01-01'                       # 回测起始时间
end = '2020-06-01'                         # 回测结束时间
universe = DynamicUniverse('HS300') + ['IFL0','IFL1']        # 证券池，支持股票、基金、期货、指数四种资产
benchmark = 'HS300'                        # 策略参考标准
freq = 'd'                                 
refresh_rate = 1  
capital_base = 10000000
cal = Calendar('China.SSE')
period = Period('-1B')

#配置账户信息，支持多资产多账户
stock_commission = Commission(buycost=0.0, sellcost=0.0, unit='perValue')
fututes_commission = Commission(buycost=0.0, sellcost=0.0, unit='perValue')
slippage = Slippage(value=0, unit='perValue')

accounts = {
    'stock_account': AccountConfig(account_type='security',capital_base=capital_base,commission=stock_commission,slippage=slippage),
    'futures_account': AccountConfig(account_type='futures',capital_base=capital_base,commission=fututes_commission,slippage=slippage)
}
  
def initialize(context):
    context.signal_generator = SignalGenerator(Signal('NetProfitGrowRate'),Signal('ROE'),Signal('RSI'))
    context.need_to_switch_position = False
    context.contract_holding = ''
    pass
  
    
def handle_data(context):   
    universe = context.get_universe(exclude_halt=True)
    yesterday = context.previous_date
    signal_composite = DataFrame()
    
    # 净利润增长率
    NetProfitGrowRate = context.signal_result['NetProfitGrowRate']
    signal_NetProfitGrowRate = standardize(neutralize(winsorize(NetProfitGrowRate),yesterday.strftime('%Y%m%d')))
    signal_composite['NetProfitGrowRate'] = signal_NetProfitGrowRate
    
    # 权益收益率
    ROE = context.signal_result['ROE']
    signal_ROE = standardize(neutralize(winsorize(ROE),yesterday.strftime('%Y%m%d')))
    signal_composite['ROE'] = signal_ROE

    # RSI
    RSI = context.signal_result['RSI']
    signal_RSI = standardize(neutralize(winsorize(RSI),yesterday.strftime('%Y%m%d')))
    signal_composite['RSI'] = signal_RSI

    # 信号合成，各个因子权重
    weight = np.array([0.6,0.3,0.1])
    signal_composite['total_score'] = np.dot(signal_composite, weight)
    
    # 订单委托
    def handle_stock_orders(context, wts):
        account = context.get_account('stock_account')
    
    def handle_futures_orders(context):
        stock_account = context.get_account('stock_account')
        future_account = context.get_account('futures_account')
    
    # 组合构建
    total_score = signal_composite['total_score'].to_dict()
    wts = simple_long_only(total_score, yesterday.strftime('%Y%m%d'))
    handle_stock_orders(context, wts)
    handle_futures_orders(context)
   # handle_futures_position_switch(context)

    account = context.get_account('stock_account')
    stock_account = context.get_account('stock_account')
    future_account = context.get_account('futures_account')
                
    log.info(u'时间：%s' % (context.current_date))
    
    # 先卖出
    sell_list = account.get_positions()
    for stk in sell_list:
        account.order_to(stk,0)
    
    # 再买入
    buy_list = wts.keys()
    total_money = account.portfolio_value
    prices = account.reference_price
    for stk in buy_list:
        if np.isnan(prices[stk]) or prices[stk] == 0:  # 停牌或者还没有上市等原因不能交易
            continue
        account.order(stk, int(total_money * wts[stk] / prices[stk] / 100) * 100)
    
    # 将主力连续合约映射为实际合约
    contract_current_month = context.get_symbol('IFL0')
    
    # 判断是否需要移仓换月
    contract_holding = context.contract_holding
    if not contract_holding:
        contract_holding = contract_current_month
    
    if contract_holding: # 这里还需要判断？什么情况没有？
        last_trade_date = get_asset(contract_holding).last_trade_date
        
        # 当月合约离交割日只有3天
        days_to_expire = (last_trade_date - context.current_date).days
        if days_to_expire == 3:
            log.info(u'距离%s到期，还有%s天' % (contract_holding, days_to_expire))
            contract_next_month = context.get_symbol('IFL1')
            futures_position = future_account.get_position(contract_holding)
            if futures_position:
                current_holding = futures_position.short_amount
                log.info(u'移仓换月。【平仓旧合约：%s，开仓新合约：%s，手数：%s】' % (contract_holding, contract_next_month, int(current_holding)))
                
                if current_holding == 0: # 注意这里的判断
                    return
                future_account.order(contract_holding,current_holding,'close')
                future_account.order(contract_next_month,-1*current_holding,'open')
                context.contract_holding = contract_next_month
                return
        
    stock_position = stock_account.get_positions()
    
    # 有多头股票仓位，使用期货进行空头对冲
    if stock_position:
        stock_positions_value = stock_account.portfolio_value - stock_account.cash # 当前股票多头市值
        futures_position = future_account.get_position(contract_holding)
        if not futures_position: # 没有空头持仓，则建仓进行对冲
            contract_current_month = context.get_symbol('IFL0')
            multiplier = get_asset(contract_current_month).multiplier
            futures_price = context.current_price(contract_current_month)
            total_hedging_amount = int(stock_positions_value / futures_price / multiplier)
            future_account.order(contract_current_month,-1*total_hedging_amount,'open')
            context.contract_holding = contract_current_month
        else: # 已经有空头持仓， 则判断是否需要调仓
            contract_holding = context.contract_holding
            contract_current_month = context.get_symbol('IFL0')
            futures_price = context.current_price(contract_current_month)
            multiplier = get_asset(contract_holding).multiplier
            # 计算当前对冲需要的期货手数
            total_hedging_amount = int(stock_positions_value / futures_price / multiplier)
            hedging_amount_diff = total_hedging_amount - futures_position.short_amount
            # 调仓阀值，可以适当放大，防止反复调仓
            threshold = 2
            if hedging_amount_diff >= threshold:
                future_account.order(contract_holding,-1*int(hedging_amount_diff),'open')
            elif hedging_amount_diff <= -threshold:
                log.info(u'hedging_amount_diff:%s abs:%s' % (hedging_amount_diff, abs(hedging_amount_diff)))
                future_account.order(contract_holding,int(abs(hedging_amount_diff)),"close")  
            