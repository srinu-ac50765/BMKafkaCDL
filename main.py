import cx_Oracle
import streamlit as st
from tabulate import tabulate
import pandas as pd

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
            print("Connetion success")
            return conn


def get_co_ref_details(cursor, sql):
    complete_table_rows = []
    for row in cursor.execute(sql):
        complete_table_rows.append(row)
    col_names = [row[0] for row in cursor.description]
    return tabulate(complete_table_rows, col_names, colalign=("left",), tablefmt="grid"), col_names, complete_table_rows


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
                tabulate_result, _ , _ = get_co_ref_details(cursor, sql)
                st.text(tabulate_result)
            elif topic_name == 'bmom.co.account':
                sql = f"""SELECT * FROM BMKAFKO.BMOM_CO_ACCOUNT 
                               WHERE BAN ='{input_num}'"""
                tabulate_result, _, _ = get_co_ref_details(cursor, sql)
                st.text(tabulate_result)
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
                sql = f"""SELECT MESSAGE_TYPE, COUNT(*) AS NUM_OF_MESSAGES 
                                FROM BMKAFKO.BMOM_CO_ORDER_REFERENCE 
                                WHERE SYS_CREATION_DATE >= TO_DATE('{from_date}', 'yyyy/mm/dd')
                                AND SYS_CREATION_DATE <= TO_DATE('{to_date}', 'yyyy/mm/dd')
                                GROUP BY MESSAGE_TYPE"""
                tabulate_result, col_names, data_rows = get_co_ref_details(cursor, sql)
                st.text(tabulate_result)
                data_frame = pd.DataFrame(data=data_rows, columns=col_names)
                new_df = data_frame.reset_index().set_index('MESSAGE_TYPE')
                st.text('From dataframe')
                st.text(new_df)
                print('plotting dataframe')
                st.bar_chart(new_df)
            else:
                st.text('to_date should be greater than from_date')
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