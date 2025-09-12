# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 14:25:01 2025

@author: kongmadai
"""

r'''
-------------------------------------------------------------------------------
安装开发环境install Spyder5.5.1 from software center
因为没有管理员权限，所以创建虚拟环境：
"C:\Program Files\Python312\python.exe" -m venv C:\Users\user\spyder-env
激活虚拟环境：
C:\Users\user\spyder-env\Scripts\activate
安装 spyder-kernels：
pip install spyder-kernels==2.5.*
配置 Spyder 使用虚拟环境：
打开 Tools（工具） > Preferences（首选项） > Python Interpreter（Python 解释器）。
设置为 C:\Users\user\spyder-env\Scripts\python.exe 
之后，Spyder 会使用系统 Python 及其包管理。
-------------------------------------------------------------------------------
need the following packages:  安装依赖包  
C:\Users\user\spyder-env\Scripts\activate   激活虚拟环境
pip install akshare numpy
-------------------------------------------------------------------------------
'''

import akshare as ak
import time
import numpy as np
import concurrent.futures
import threading

def get_ROE(date="20221231", max_retries=10):
    """
    获取指定日期的股票净资产收益率(ROE)数据
    
    Parameters:
    date (str): 财报日期，格式为YYYYMMDD
    max_retries (int): 最大重试次数
    
    Returns:
    dict: 股票代码为key，ROE为value的字典
    """
    attempt = 0
    
    while attempt < max_retries:
        try:
            print(f"正在获取 {date} 的ROE数据，尝试第 {attempt + 1} 次...")
            stock_yjbb_em_df = ak.stock_yjbb_em(date)
            
            # 创建字典：股票代码为key，ROE为value
            roe_dict = {}
            for _, row in stock_yjbb_em_df.iterrows():
                stock_code = row['股票代码']
                roe_value = row['净资产收益率']  # 根据实际列名调整
                roe_dict[stock_code] = roe_value
            
            print(f"成功获取 {len(roe_dict)} 只股票的ROE数据")
            return roe_dict
            
        except Exception as e:
            attempt += 1
            print(f"第 {attempt} 次尝试失败，错误信息: {str(e)}")
            
            if attempt >= max_retries:
                print(f"已达到最大重试次数 {max_retries}，获取数据失败")
                return {}
            
            # 等待一段时间后重试（指数退避策略）
            wait_time = 1 ** attempt
            print(f"等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)
    
    return {}

def get_multi_year_ROE():
    """
    获取过去五年的ROE数据并生成汇总字典
    
    Returns:
    dict: 股票代码为key，ROE列表为value的字典
    """
    # 定义要获取的年份列表
    years = ["20201231", "20211231", "20221231", "20231231", "20241231", "20250630"]
    
    # 获取各年份的ROE数据
    roe_data_by_year = {}
    for year in years:
        roe_data = get_ROE(date=year, max_retries=10)
        roe_data_by_year[year] = roe_data
        print(f"已获取 {year} 年数据，包含 {len(roe_data)} 只股票")
    
    # 获取所有股票代码的全集
    all_stock_codes = set()
    for year_data in roe_data_by_year.values():
        all_stock_codes.update(year_data.keys())
    
    print(f"总共找到 {len(all_stock_codes)} 只股票")
    
    # 创建结果字典
    result_dict = {}
    
    for stock_code in all_stock_codes:
        roe_list = []
        
        # 按年份顺序获取ROE值
        for year in years:
            if stock_code in roe_data_by_year[year]:
                roe_value = roe_data_by_year[year][stock_code]
                roe_list.append(roe_value)
            else:
                roe_list.append(np.nan)  # 用NaN代替缺失数据
                
        #修正第六个:半年报
        roe_list[-1] = roe_list[-1] / 2 * 4
        
        # 计算平均值（忽略NaN）
        valid_values = [x for x in roe_list if not np.isnan(x)]
        if valid_values:
            avg_roe = sum(valid_values) / len(valid_values)
        else:
            avg_roe = np.nan
        
        # 添加平均值到列表末尾  :第7个
        roe_list.append(avg_roe)
        
        result_dict[stock_code] = roe_list
    
    result_dict_ = {}
    for stock_code in result_dict:
        # 舍弃 8开头的 新三板  
        if stock_code.startswith('8'):
            pass
        elif stock_code.startswith('2'):
            pass
        elif stock_code.startswith('9'):
            pass
        elif stock_code.startswith('4'):
            pass
        else:
            result_dict_[stock_code] = result_dict[stock_code]
            
    return result_dict_ , years

  
def clean_data_ROE_v2():
    """
    清洗多年度ROE数据，筛选符合条件的数据
    重新计算平均值，确保使用所有可用年份的数据
    
    Returns:
    dict: 符合条件的数据字典
    int: 原始数据总量
    int: 筛选后数据量
    """
    # 获取多年度ROE数据
    multi_year_roe, year_list = get_multi_year_ROE()
    
    print(f"\n年份顺序: {year_list}")
    print(f"总共处理了 {len(multi_year_roe)} 只股票")
    
    # 筛选符合条件的数据
    filtered_data = {}
    
    for stock_code, roe_list in multi_year_roe.items():
        # 获取前5年的有效数据（忽略NaN）
        valid_values = [x for x in roe_list[:-1] if not np.isnan(x)]
        
        if not valid_values:  # 如果没有有效数据，跳过
            continue
        
        # 重新计算平均值（使用所有可用年份）
        avg_roe = sum(valid_values) / len(valid_values)
        
        '''# 条件1：平均值必须大于1%
        '''
        if avg_roe <= 1:
            continue

        
        '''# 计算最小值  必须大于-5%
        '''
        min_roe = min(valid_values)
        
        if min_roe <= -5:
            continue
        
        '''# 条件2：最小值乘以80必须大于平均值
        '''
        if abs(min_roe) * 80 > avg_roe or min_roe * 80 > avg_roe :
            # 创建新的ROE列表，包含原始数据和重新计算的平均值
            new_roe_list = roe_list.copy()
            new_roe_list[-1] = avg_roe  # 更新平均值
            filtered_data[stock_code] = new_roe_list
    
    print(f"\n筛选后数据统计信息:")
    print(f"筛选后股票数量: {len(filtered_data)}")
    
    # 统计有效数据年数分布
    # year_distribution = {}
    # for roe_list in filtered_data.values():
    #     valid_count = sum(1 for x in roe_list[:5] if not np.isnan(x))
    #     year_distribution[valid_count] = year_distribution.get(valid_count, 0) + 1
    
    # print(f"\n有效数据年数分布:")
    # for years, count in sorted(year_distribution.items()):
    #     print(f"  {years}年有效数据: {count}只股票 ({count/len(filtered_data)*100:.1f}%)")
    
    return filtered_data, len(multi_year_roe), len(filtered_data)


def get_hangye(stock_code="000001"):#行业
    try:
        stock_individual_info_em_df = ak.stock_individual_info_em(stock_code)
        print(stock_individual_info_em_df)
        data_dict = dict(zip(stock_individual_info_em_df['item'], stock_individual_info_em_df['value']))

        hangye = str(data_dict.get('行业', np.nan))
        
        return hangye
    except:
        pass 
    
    return np.nan
    

def safe_convert(value, default=np.nan):
    """
    安全转换函数，处理None、字符串等非数值数据
    """
    try:
        if value is None or value == 'None' or value == '' or str(value).strip() == '-':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def get_stock_metrics(stock_code):
    """
    获取单只股票的市盈率、股息率和市净率
    
    Parameters:
    stock_code (str): 股票代码
    
    Returns:
    tuple: (市盈率, 股息率, 市净率) 或 (np.nan, np.nan, np.nan) 如果获取失败
    
    例如 get_stock_metrics('002202') = (17.226, 1.154, 1.371)
    """
    symbols_to_try = []
    
    # 根据股票代码判断可能的交易所
    if stock_code.startswith('6'):
        symbols_to_try.append(f"SH{stock_code}")
        symbols_to_try.append(f"SZ{stock_code}")
        symbols_to_try.append(f"SH{stock_code}")
        symbols_to_try.append(f"SZ{stock_code}")
    elif stock_code.startswith(('0', '3')):
        symbols_to_try.append(f"SZ{stock_code}")
        symbols_to_try.append(f"SH{stock_code}")
        symbols_to_try.append(f"SZ{stock_code}")
        symbols_to_try.append(f"SH{stock_code}")
    elif stock_code.startswith('8'):
        symbols_to_try.append(f"BJ{stock_code}")
        symbols_to_try.append(f"SZ{stock_code}")
        symbols_to_try.append(f"SH{stock_code}")
    else:
        symbols_to_try.append(f"SH{stock_code}")
        # symbols_to_try.append(f"SZ{stock_code}")
        # symbols_to_try.append(f"BJ{stock_code}")
    
    # 尝试获取股票指标数据
    for symbol in symbols_to_try:
        try:
            print(f"当前尝试 {symbol}  ")
            stock_individual_spot_xq_df = ak.stock_individual_spot_xq(symbol=symbol)
            data_dict = dict(zip(stock_individual_spot_xq_df['item'], stock_individual_spot_xq_df['value']))
            
            # 使用安全转换函数处理数据
            pe_ratio = safe_convert(data_dict.get('市盈率(动)', np.nan))
            dividend_yield = safe_convert(data_dict.get('股息率(TTM)', np.nan))
            pb_ratio = safe_convert(data_dict.get('市净率', np.nan))
            
            # 处理股票名称
            stockname = data_dict.get('名称', '')
            if stockname in [None, 'None', '']:
                stockname = np.nan
            else:
                stockname = str(stockname)
            
            time.sleep(0.1)
            return pe_ratio, dividend_yield, pb_ratio, stockname
            
        except Exception as e:
            continue
        
        # 添加短暂延迟
        time.sleep(0.1)
    
    return np.nan, np.nan, np.nan, np.nan


def get_stock_metrics_wrapper(stock_code):
    """
    包装函数，用于线程池调用
    """
    return stock_code, get_stock_metrics(stock_code)


def append_pb():
    """
    在ROE数据基础上添加市净率、市盈率、股息率和性价比指标
    
    Returns:
    dict: 包含PB、PE、股息率和性价比指标的增强数据字典
    int: 原始股票数量
    int: 成功获取指标数据的股票数量
    """
    # 首先获取清洗后的ROE数据
    filtered_data, original_count, filtered_count = clean_data_ROE_v2()
    
    if not filtered_data:
        print("没有可处理的ROE数据")
        return {}, 0, 0
    
    print(f"\n开始为 {len(filtered_data)} 只股票获取市净率、市盈率和股息率数据...")
    
    # 创建增强数据字典
    enhanced_data = {}
    success_count = 0
    fail_count = 0
    
    # 使用线程池并发获取股票指标
    stock_codes = list(filtered_data.keys())
    results = {}
    
    # 创建线程池，最大线程数为20
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # 提交所有任务
        future_to_stock = {executor.submit(get_stock_metrics_wrapper, code): code for code in stock_codes}
        
        # 处理完成的任务
        for future in concurrent.futures.as_completed(future_to_stock):
            stock_code = future_to_stock[future]
            try:
                result_code, metrics = future.result()
                results[stock_code] = metrics
                print(f"已获取 {len(results)}/{len(stock_codes)} 只股票数据")
                
            except Exception as e:
                print(f"处理股票 {stock_code} 时出错: {str(e)}")
                results[stock_code] = (np.nan, np.nan, np.nan, np.nan)
    
    # 处理获取到的指标数据
    success_count = 0
    for stock_code, roe_list in filtered_data.items():
        if stock_code not in results:
            continue
            
        pe_ratio, dividend_yield, pb_ratio, stockname = results[stock_code]
        
        # 检查数据有效性 - 修复：使用更安全的方式检查
        def is_valid_numeric(value):
            """检查数值是否有效"""
            try:
                return not np.isnan(float(value)) and float(value) > 0
            except (ValueError, TypeError):
                return False
        
        def is_valid_string(value):
            """检查字符串是否有效"""
            return value is not np.nan and value not in [None, 'None', ''] and isinstance(value, str)
        
        # 使用安全的检查方式
        if (not is_valid_numeric(pe_ratio) or 
            not is_valid_numeric(dividend_yield) or 
            not is_valid_numeric(pb_ratio) or 
            not is_valid_string(stockname)):
            continue

            
#  ak.stock_individual_spot_xq('SZ002681')
# Out[8]: 
#         item                value
# 0         代码             SZ002681
# 1      52周最高                10.49
# 2        流通股           1532680780
# 3         跌停                 6.74
# 4         最高                  7.6
# 5        流通值        11633047120.0
# 6     最小交易单位                  100
# 7         涨跌                  0.1
# 8       每股收益                 0.02
# 9         昨收                 7.49
# 10       成交量             98726025
# 11       周转率                 6.44
# 12     52周最低                 3.61
# 13        名称                 奋达科技
# 14       交易所                   SZ
# 15    市盈率(动)              398.578
# 16  基金份额/总股本           1794652232
# 17   净资产中的商誉                 None
# 18        均价                7.484
# 19        涨幅                 1.34
# 20        振幅                 4.01
# 21        现价                 7.59
# 22    今年以来涨幅                21.63
# 23      发行日期  2012-06-05 00:00:00
# 24        最低                  7.3
# 25  资产净值/总市值        13621410441.0
# 26   股息(TTM)                 None   <<<<<<<<<<<<<<<<<  有的垃圾公司这里股息是None  会获取失败
# 27  股息率(TTM)                 None
# 28        货币                  CNY
# 29     每股净资产               1.3341
# 30    市盈率(静)              140.297
# 31       成交额         738823797.88
# 32       市净率                5.689
# 33        涨停                 8.24
# 34  市盈率(TTM)              485.968
# 35        时间  2025-09-11 15:04:09
# 36        今开                 7.39   
    
    
        try:
            # 提取所需指标
            # pe_ratio = float(data_dict.get('市盈率(动)', np.nan))  # 市盈率(动)
            # dividend_yield = float(data_dict.get('股息率(TTM)', np.nan))  # 股息率(TTM)
            # pb_ratio = float(data_dict.get('市净率', np.nan))  # 市净率
            
            # 获取平均ROE（原列表的第7个元素）
            avg_roe = roe_list[-1]
            # recent_roe = roe_list[-2]
            
            '''# 计算性价比指标（平均ROE/6% / 市净率）+  动态市盈率的倒数 / 6%'''
            
            
            if pb_ratio > 0:
                # value_ratio = ( (avg_roe + recent_roe) /12 ) / pb_ratio
                value_ratio = (avg_roe /12 )/ pb_ratio + (100 / pe_ratio) / 12
            else:
                value_ratio = np.nan
                
            # 创建新的数据列表（原ROE数据 + 新指标）
            enhanced_list = roe_list.copy()
            enhanced_list.extend([pe_ratio, dividend_yield, pb_ratio, value_ratio, stockname])
            
            # 添加到结果字典
            if not np.isnan(value_ratio):
                '''       
                # 市盈率 小于 200 
                # 股息率 大于 0.1%  
                # 市净率 小于33   '''    
                
                if pe_ratio < 200 and dividend_yield > 0.1 and pb_ratio < 33:
                    enhanced_data[stock_code] = enhanced_list
                    print(f"累计 {len(enhanced_data)} 符合标准: 最新的是{stockname}")
                    
                    # if success_count > 20:
                    #     return enhanced_data, len(filtered_data), success_count
            
        except Exception as e:
            print(f"处理股票 {stock_code} 时出错: {str(e)}")
            continue

    success_count = len(enhanced_data)
    return enhanced_data, len(filtered_data), success_count


# 使用示例 - 详细版本
if __name__ == "__main__":
    enhanced_data, original_count, success_count = append_pb()
    
    if enhanced_data:
        # 可以按性价比排序
        sorted_stocks = sorted(
            [(code, data[10]) for code, data in enhanced_data.items() if not np.isnan(data[10])],
            key=lambda x: x[1],
            reverse=True
        )
        
        print(f"\n性价比最高的前100只股票:")
        for i, (code, value_ratio) in enumerate(sorted_stocks[:100]):
            print(f"{i+1}. {code}: {value_ratio:.3f}")
        
        # 输出到txt文件
        output_filename = "stock_analysis_results.txt"
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            # 写入文件头信息
            f.write("=" * 80 + "\n")
            f.write("股票分析结果 - 按性价比排序\n")
            f.write("=" * 80 + "\n")
            f.write(f"分析时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"总股票数量: {original_count} (沪深: 持续五年盈利， 平均ROE大于3% )  \n")
            f.write(f"筛选后股票数量: {len(enhanced_data)} (市盈率 小于 200 ; 股息率 大于 0.1% ;  市净率 小于33  ) \n")   
            f.write(f"输出排名1500只股票\n")
            f.write("=" * 80 + "\n\n")
            
            # 写入表头
            f.write("排名,代码,公司名称,行业,ROE20年报,ROE21年报,ROE22年报,ROE23年报,ROE24年报,ROE25半年报,,平均ROE,,市盈率动,,股息率TTM,,市净率,,性价比\n")
            
            # 写入前1500个结果
            for i, (code, value_ratio) in enumerate(sorted_stocks[:1500]):
                if code in enhanced_data:
                    data = enhanced_data[code]
                    company_name = data[11] if len(data) > 11 else "未知公司"
                    company_hangye = get_hangye(code)
                    
                    # 格式化数据行
                    row_data = [
                        f"{i+1}",  # 排名
                        "'"+str(code),  # 代码
                        str(company_name),  # 公司名称
                        str(company_hangye),  # 公司行业
                        *[f"{x:.2f}" if not np.isnan(x) else "NaN" for x in data[:6]],  # ROE2020-ROE2024 + 2025
                        f"平均,{data[6]:.2f}" if not np.isnan(data[6]) else "NaN",  # 平均ROE
                        f"市盈,{data[7]:.2f}" if not np.isnan(data[7]) else "NaN",  # 市盈率
                        f"股息,{data[8]:.2f}" if not np.isnan(data[8]) else "NaN",  # 股息率
                        f"市净,{data[9]:.2f}" if not np.isnan(data[9]) else "NaN",  # 市净率
                        f"价值,{data[10]:.3f}" if not np.isnan(data[10]) else "NaN"   # 性价比
                    ]
                    
                    # 写入文件
                    f.write(",".join(row_data) + "\n")
        
        print(f"\n结果已保存到文件: {output_filename}")

        
# if __name__ == "__main__":   # 调试用
#     output_filename = "stock_analysis_results.txt"    
#     with open(output_filename, 'w', encoding='utf-8') as f:
#         f.write(f"分析时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
#         f.write(f"输出排名1500只股票\n")
#     print(f"\n结果已保存到文件: {output_filename}")
