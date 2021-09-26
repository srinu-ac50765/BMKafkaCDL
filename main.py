import cx_Oracle
import streamlit as st
from tabulate import tabulate

lib_dir = r"C:\Software\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11"
st.set_page_config(page_title="BMKafkaCDL", page_icon=":mailbox_with_mail:", layout='wide',
                   initial_sidebar_state='collapsed')
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


def get_co_ref_details(cursor, input_num, sql):
    complete_table_rows = []
    for row in cursor.execute(sql):
        complete_table_rows.append(row)
    col_names = [row[0] for row in cursor.description]
    return tabulate(complete_table_rows, col_names, colalign=("left",), tablefmt="grid")


def search_functionality():
    conn = None
    cursor = None
    try:
        conn = get_connection()
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
            cursor = conn.cursor()
            if topic_name == 'bmom.co.orderReference':
                sql = """SELECT * FROM BMKAFKO.BMOM_CO_ORDER_REFERENCE BCOR
                               LEFT JOIN BMKAFKO.BMOM_CO_ORDER_REF_RMK BCORR
                               ON BCOR.ORDER_REF_ID = BCORR.ORDER_REF_ID 
                               LEFT JOIN BMKAFKO.BMOM_CO_ORDER_REF_RSN BCORR2
                               ON BCOR.ORDER_REF_ID = BCORR2.ORDER_REF_ID 
                               WHERE BCOR.BAN ='""" + input_num + "'"
                st.text(get_co_ref_details(cursor, input_num, sql))
            elif topic_name == 'bmom.co.account':
                sql = """SELECT * FROM BMKAFKO.BMOM_CO_ACCOUNT 
                               WHERE BAN ='""" + input_num + "'"
                st.text(get_co_ref_details(cursor, input_num, sql))
    except cx_Oracle.DatabaseError as exc:
        err, = exc.args
        print("Oracle-Error-Code:" + str(err.code))
        print("Oracle-Error-Message:" + err.message)
    except Exception as er:
        print('Error:' + str(er))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def statistics_functionality():
    pass


def print_hi():
    search_order = st.radio('', ('Search Order', 'Message Statistics'))
    if search_order == 'Search Order':
        search_functionality()
    elif search_order == 'Message Statistics':
        statistics_functionality()


if __name__ == '__main__':
    print_hi()
