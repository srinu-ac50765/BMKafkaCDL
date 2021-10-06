import cx_Oracle
import streamlit as st
from tabulate import tabulate
import pandas as pd
from random import random

lib_dir = r"C:\Software\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11"
st.set_page_config(page_title="BMKafkaCDL", page_icon=":mailbox_with_mail:", layout='wide',
                   initial_sidebar_state='collapsed')
st.subheader('App to explore BM to CDL kafka messages')
oracle_db_server = 'lxmdtsdxdp2-scan.test.intranet'
oracle_db_port = '1521'
oracle_db_service = 'ENS_DEP2'
oracle_db_user = r'BMKAFKC'
oracle_db_passwd = 'BMP583SL'

st.markdown(
    """
    <style>
     #MainMenu {visibility: hidden;}
     footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True
)


def get_connection():
    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        dsn_tns = cx_Oracle.makedsn(oracle_db_server, oracle_db_port, service_name=oracle_db_service)
        conn = cx_Oracle.connect(user=oracle_db_user, password=oracle_db_passwd, dsn=dsn_tns)
        print("Connetion success")
        return conn
    except cx_Oracle.DatabaseError as exc:
        err, = exc.args
        print('Error from get connection method, code : ' + str(err.code) + ' message: ' + err.message)
        if err.code == 0:
            print('Error code 0 happened, proceeding with opening connection ..')
            dsn_tns = cx_Oracle.makedsn(oracle_db_server, oracle_db_port, service_name=oracle_db_service)
            conn = cx_Oracle.connect(user=oracle_db_user, password=oracle_db_passwd, dsn=dsn_tns)
            print("Connection success")
            return conn


def get_details_from_db(cursor, sql):
    complete_table_rows = []
    for row in cursor.execute(sql):
        complete_table_rows.append(row)
    col_names = [row[0] for row in cursor.description]
    return tabulate(complete_table_rows, col_names, colalign=("left",), tablefmt="grid"), col_names, complete_table_rows


def getDataFrame(col_names, data_rows):
    de_dup_col_names = []
    for col_name in col_names:
        if col_name not in de_dup_col_names:
            de_dup_col_names.append(col_name)
        else:
            de_dup_col_names.append(col_name + '_' + str(random() * 100000)[0:5])
    return pd.DataFrame(
        data_rows,
        columns=de_dup_col_names)


def search_functionality(conn):
    col1, col2, _, _, _, _ = st.columns(6)
    with col1:
        input_num = st.text_input('BAN :', max_chars=11)
    with col2:
        topic_name = st.selectbox('Topic: ', ('bmom.co.orderReference',
                                              'bmom.co.account',
                                              'bmom.co.address',
                                              'bmom.co.orderItems',
                                              'bmom.co.schedule',
                                              'bmom.co.orderDealerCode',
                                              'bmom.co.pricingDetails',
                                              'bmom.co.salesChannelInfo',
                                              'bmom.bo.productDetails',
                                              'bmom.bo.discount',
                                              'bmom.bo.otc',
                                              'bmom.bo.prepaidProductDetails',
                                              'bmom.po.provServiceOrderStatus',
                                              'bmom.po.tomProvisioned',
                                              'bmom.po.dhpProvisioned',
                                              'bmom.po.ffwfProvisioned',
                                              'bmom.po.dtvProvisioned',
                                              'bmom.po.directoryListing',
                                              'bmom.po.tomAsyncResponseLC',
                                              'bmom.po.tomAsyncResponseLQ'))

    enter_button = st.button('Search')
    if enter_button and input_num:
        cursor = None
        try:
            cursor = conn.cursor()
            if topic_name == 'bmom.co.orderReference':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_ORDER_REFERENCE BCOR
                               LEFT JOIN BMKAFKO.BMOM_CO_ORDER_REF_RMK BCORR
                               ON BCOR.ORDER_REF_ID = BCORR.ORDER_REF_ID 
                               LEFT JOIN BMKAFKO.BMOM_CO_ORDER_REF_RSN BCORR2
                               ON BCOR.ORDER_REF_ID = BCORR2.ORDER_REF_ID 
                               WHERE BCOR.BAN ='{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.co.account':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_ACCOUNT 
                               WHERE BAN ='{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.co.address':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_ADDRESS bca 
                            LEFT JOIN BMKAFKO.BMOM_CO_ADDRESS_DETAIL bcad 
                            ON bca.SVC_ADDRESS_ID = bcad.ADDRESS_ID 
                            OR bca.BILLING_ADDRESS_ID  = BCAD.ADDRESS_ID 
                            WHERE bca.BAN ='{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.co.orderItems':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_ORDER_ITEMS bcoi  
                            LEFT JOIN BMKAFKO.BMOM_CO_ORDER_ITEMS_DET bcoid  
                            ON bcoi.PARENT_ORDER_ITEM_ID = bcoid.PARENT_ORDER_ITEM_ID 
                            LEFT JOIN BMKAFKO.BMOM_CO_RESERVED_TN bcrt   
                            ON bcoi.PARENT_ORDER_ITEM_ID = bcrt.PARENT_ORDER_ITEM_ID 
                            WHERE bcoi.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.co.schedule':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_SCHEDULE bcs   
                            LEFT JOIN BMKAFKO.BMOM_CO_SCHEDULE_RSN bcsr   
                            ON bcs.SCHEDULE_ID = bcsr.SCHEDULE_ID 
                            WHERE bcs.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.co.orderDealerCode':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_ORDER_DEALER_CODE bcodc    
                            LEFT JOIN BMKAFKO.BMOM_CO_ORDER_DEALER_CODE_DET bcodcd    
                            ON bcodc.PARENT_DEALER_CODE_ID = bcodcd.PARENT_DEALER_CODE_ID 
                            WHERE bcodc.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.co.pricingDetails':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_PRICING bcp     
                            LEFT JOIN BMKAFKO.BMOM_CO_PRICING_DETAIL bcpd     
                            ON bcp.PRICING_ID = bcpd.PRICING_ID 
                            WHERE bcp.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.co.salesChannelInfo':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_SALES_CHANNEL bcsc      
                            LEFT JOIN BMKAFKO.BMOM_CO_SALES_CHANNEL_ATTR bcsca      
                            ON bcsc.SALES_CHANNEL_ID = bcsca.SALES_CHANNEL_ID 
                            WHERE bcsc.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.bo.productDetails':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_BO_BLNG_PRODUCTS bbbp       
                            LEFT JOIN BMKAFKO.BMOM_BO_BLNG_PRODUCT_DETAILS bbbpd       
                            ON bbbp.BLNG_PRODUCT_ID = bbbpd.BLNG_PRODUCT_ID 
                            LEFT JOIN BMKAFKO.BMOM_BO_BLNG_PRICE_PLAN bbbpp        
                            ON bbbp.BLNG_PRODUCT_ID = bbbpp.BLNG_PRODUCT_ID 
                            LEFT JOIN BMKAFKO.BMOM_BO_BLNG_FEATURE_CODE bbbfc         
                            ON bbbp.BLNG_PRODUCT_ID = bbbfc.BLNG_PRODUCT_ID 
                            WHERE bbbp.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.bo.discount':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_BO_DISCOUNTS bbd        
                            LEFT JOIN BMKAFKO.BMOM_BO_DISCOUNT_DETAIL bbdd        
                            ON bbd.DISCOUNT_ID = bbdd.DISCOUNT_ID 
                            WHERE bbd.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.bo.otc':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_BO_OTC_PRODUCTS bbop         
                            LEFT JOIN BMKAFKO.BMOM_BO_OTC_PRODUCT_DET bbopd         
                            ON bbop.OTC_PRODUCT_ID = bbopd.OTC_PRODUCT_ID 
                            WHERE bbop.BAN ='{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.bo.prepaidProductDetails':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_BO_PREPAID_PRODUCTS bbpp          
                            LEFT JOIN BMKAFKO.BMOM_BO_PREPAID_PRODUCT_DET bbppd          
                            ON bbpp.PREPAID_PRODUCT_ID = bbppd.PREPAID_PRODUCT_ID 
                            WHERE bbpp.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.po.provServiceOrderStatus':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_PO_PROV_SVC_ORDER_STATUS 
                            WHERE BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.po.tomProvisioned':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_PO_TOM_PROVISIONED bptp           
                            LEFT JOIN BMKAFKO.BMOM_PO_TOM_PROV_PRODUCT_LIST bptppl           
                            ON bptp.TOM_PROVISIONED_ID = bptppl.TOM_PROVISIONED_ID 
                            WHERE bptp.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.po.dhpProvisioned':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_PO_DHP_PROVISIONED bpdp 
                            WHERE BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.po.ffwfProvisioned':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_PO_FFWF_PROVISIONED bpfp            
                            LEFT JOIN BMKAFKO.BMOM_PO_FFWF_PROVISIONED_DET bpfpd            
                            ON bpfp.PARENT_FFWF_PROV_ID = bpfpd.PARENT_FFWF_PROV_ID 
                            LEFT JOIN BMKAFKO.BMOM_PO_ENJ_PARAMETERS bpep             
                            ON bpfpd.FFWF_PROV_ID = bpep.FFWF_PROV_ID 
                            WHERE bpfp.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.po.dtvProvisioned':
                st.text('Table was not created for DTV')
            elif topic_name == 'bmom.po.directoryListing':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_PO_DIRECTORY_LISTING bpdl  
                            WHERE BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.po.tomAsyncResponseLC':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_PO_TOM_ASYNCH_RESPONSE_LC bptarl             
                            LEFT JOIN BMKAFKO.BMOM_PO_MOI_SERVICE_ORDER bpmso             
                            ON bptarl.TOM_ASYNCH_RESPLC_ID = bpmso.TOM_ASYNCH_RESPLC_ID 
                            WHERE bptarl.BAN = '{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
            elif topic_name == 'bmom.po.tomAsyncResponseLQ':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_PO_TOM_ASYNCH_RESPONSE bptar              
                            LEFT JOIN BMKAFKO.BMOM_PO_PROV_SERVICE_ORDER bppso              
                            ON bptar.TOM_ASYNCH_RESP_ID = bppso.TOM_ASYNCH_RESP_ID 
                            WHERE bptar.BAN ='{input_num}'"""
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.dataframe(getDataFrame(col_names, data_rows))
        except Exception as er:
            print('Error opening cursor:' + str(er))
        finally:
            if cursor:
                cursor.close()


def statistics_functionality(conn):
    col1, col2, col3, _, _, _ = st.columns(6)
    with col1:
        st.text('Select date range :')
    with col2:
        from_date = st.date_input('from_date')
    with col3:
        to_date = st.date_input('to_date')

    enter_button = st.button('Search')
    if enter_button and from_date and to_date:
        print(from_date)
        print(to_date)
        cursor = None
        try:
            if to_date > from_date:
                cursor = conn.cursor()
                sql = f"""SELECT MESSAGE_TYPE,  COUNT(*) AS NUM_OF_MESSAGES 
                        FROM BMKAFKO.BMOM_CO_ORDER_REFERENCE 
                        WHERE SYS_CREATION_DATE >= TO_DATE('{from_date}', 'yyyy/mm/dd')
                        AND SYS_CREATION_DATE <= TO_DATE('{to_date}', 'yyyy/mm/dd')
                        GROUP BY MESSAGE_TYPE """
                tabulate_result, col_names, data_rows = get_details_from_db(cursor, sql)
                st.text('Data')
                st.text(tabulate_result)
                df = pd.DataFrame(
                    data_rows,
                    columns=col_names)
                df.index.name = 'idx_name'
                st.dataframe(df)
                print(df.index.name)
                #Removing index from df in next 2 lines
                # blankIndex = [''] * len(df)
                # df.index = blankIndex
                # new_df = df.reset_index().set_index('MESSAGE_TYPE')
                # st.text(new_df)
                st.bar_chart(df)
            else:
                st.error('"to_date" should be greater than "from_date"')
        except Exception as er:
            print('Error opening cursor:' + str(er))
        finally:
            if cursor:
                cursor.close()


def print_hi():
    search_order = st.radio('', ('Search Order', 'Message Statistics'))
    conn = None
    try:
        conn = get_connection()
        if search_order == 'Search Order':
            search_functionality(conn)
        elif search_order == 'Message Statistics':
            statistics_functionality(conn)
    except cx_Oracle.DatabaseError as exc:
        err, = exc.args
        print("Oracle-Error-Code:" + str(err.code))
        print("Oracle-Error-Message:" + err.message)
    except Exception as er:
        print('Error:' + str(er))
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    print_hi()
