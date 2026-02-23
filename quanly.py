import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
import os
import json
import re
import time
import io

# --- THƯ VIỆN KẾT NỐI GOOGLE SHEETS ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG VÀ GIAO DIỆN
# ==============================================================================

st.set_page_config(
    page_title="MT60 Cloud Manager", 
    layout="wide", 
    page_icon="☁️",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
        .block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; }
        div[data-testid="stVerticalBlock"] { gap: 0.2rem !important; }
        div[data-testid="stDataFrame"] { width: 100%; }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-thumb { background: #888; border-radius: 3px; }
    </style>
""", unsafe_allow_html=True)

try:
    from google import genai
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

SHEET_NAME = "MT60_DATABASE"

COLUMNS = [
    "Tòa nhà", "Mã căn", "Toà", "Chủ nhà - sale", "Ngày ký", "Ngày hết HĐ", 
    "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Tên khách thuê", 
    "Ngày in", "Ngày out", "Giá", "KH thanh toán", "KH cọc", 
    "Công ty", "Cá Nhân", "SALE THẢO", "SALE NGA", "SALE LINH", 
    "Hết hạn khách hàng", "Ráp khách khi hết hạn"
]

COLUMNS_CP = ["Ngày", "Mã căn", "Loại", "Tiền", "Chỉ số đồng hồ"]

COLS_MONEY = [
    "Giá", "Giá HĐ", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", 
    "Cá Nhân", "TT cho chủ nhà", "Cọc cho chủ nhà", "KH thanh toán", "KH cọc"
]

# ==============================================================================
# 2. KẾT NỐI DỮ LIỆU THÔNG MINH (TỰ ĐỘNG ĐỌC FILE KEY.JSON)
# ==============================================================================

st.title("☁️ MT60 STUDIO - QUẢN LÝ TỔNG QUAN")
st.markdown("---")

st.sidebar.header("🔐 Trạng thái hệ thống")

@st.cache_resource
def connect_google_sheet(uploaded_file=None):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_dict = None
        
        # Cách 1: Tự động tìm file "key.json" để trong cùng thư mục
        if os.path.exists("key.json"):
            with open("key.json", "r", encoding="utf-8") as f:
                creds_dict = json.load(f)
        # Cách 2: Nếu người dùng upload file
        elif uploaded_file is not None:
            file_content = uploaded_file.read().decode("utf-8")
            creds_dict = json.loads(file_content)
            
        if creds_dict:
            # Sửa lỗi ký tự xuống dòng của Google
            if 'private_key' in creds_dict:
                creds_dict['private_key'] = creds_dict['private_key'].replace('\\n', '\n')
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            return client.open(SHEET_NAME)
        return None
    except Exception as e:
        st.error(f"❌ Lỗi kết nối. Vui lòng kiểm tra lại file JSON của bạn. Chi tiết: {e}")
        return None

# --- Khởi tạo kết nối ---
sh = None
if os.path.exists("key.json"):
    with st.spinner("Đang tự động kết nối bằng file key.json..."):
        sh = connect_google_sheet()
else:
    uploaded_key = st.sidebar.file_uploader("Không tìm thấy key.json. Vui lòng Upload file JSON:", type=['json'])
    if uploaded_key:
        uploaded_key.seek(0)
        with st.spinner("Đang kết nối..."):
            sh = connect_google_sheet(uploaded_key)

# ==============================================================================
# 3. XỬ LÝ LOGIC CHÍNH
# ==============================================================================

if sh:
    st.sidebar.success("✅ Đã kết nối dữ liệu!")
    
    def load_data(tab_name):
        try:
            wks = sh.worksheet(tab_name)
            data = wks.get_all_records()
            if not data: return pd.DataFrame()
            return pd.DataFrame(data)
        except: return pd.DataFrame()

    def save_data(df, tab_name):
        try:
            wks = sh.worksheet(tab_name)
            df_save = df.fillna("") 
            df_save = df_save.astype(str)
            wks.clear()
            wks.update([df_save.columns.values.tolist()] + df_save.values.tolist())
            st.toast("✅ Đã lưu thành công!", icon="☁️")
        except Exception as e: st.error(f"❌ Lỗi: {e}")

    # --- BỘ LỌC ÉP KIỂU SỐ (NGĂN CHẶN LỖI NHÂN 10 LẦN) ---
    def clean_money(val):
        if pd.isna(val) or val == "": return 0.0
        if isinstance(val, (int, float)): return float(val)
        s = str(val).strip()
        if s.endswith('.0'): s = s[:-2]
        if s.endswith(',0'): s = s[:-2]
        s = s.replace('.', '').replace(',', '')
        s = re.sub(r'[^\d-]', '', s)
        try: return float(s)
        except: return 0.0

    # --- HÀM FORMAT HIỂN THỊ CHỐNG LỖI 2^53 ---
    def fmt_vnd(val):
        try:
            val = float(val)
            if pd.isna(val) or val == 0: return "0"
            if val < 0: return "({:,.0f})".format(abs(val)).replace(",", ".")
            return "{:,.0f}".format(val).replace(",", ".")
        except: return "0"

    def fmt_date(val):
        try:
            if pd.isna(val) or val == "": return ""
            if isinstance(val, str): val = pd.to_datetime(val, errors='coerce')
            if pd.isna(val): return ""
            return val.strftime('%d/%m/%y')
        except: return ""

    def convert_df_to_excel(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_export = df.copy()
            for col in df_export.columns:
                if pd.api.types.is_datetime64_any_dtype(df_export[col]):
                    df_export[col] = df_export[col].dt.strftime('%d/%m/%y')
            df_export.to_excel(writer, index=False, sheet_name='Sheet1')
        return output.getvalue()
    
    # --- HÀM GỘP DỮ LIỆU ---
    def gop_du_lieu_phong(df_input):
        if df_input.empty: return df_input
        df = df_input.copy()
        df.columns = df.columns.str.strip()

        def tao_mo_ta_dong(row):
            details = []
            k, h = fmt_date(row.get('Ngày ký')), fmt_date(row.get('Ngày hết HĐ'))
            i, o = fmt_date(row.get('Ngày in')), fmt_date(row.get('Ngày out'))
            if k or h: details.append(f"HĐ({k}-{h})")
            if row.get('Giá HĐ', 0) > 0: details.append(f"GiáHĐ:{fmt_vnd(row['Giá HĐ'])}")
            if i or o: details.append(f"Khách({i}-{o})")
            if row.get('Giá', 0) > 0: details.append(f"GiáThuê:{fmt_vnd(row['Giá'])}")
            
            thu = row.get('KH thanh toán', 0) + row.get('KH cọc', 0)
            if thu > 0: details.append(f"Thu:{fmt_vnd(thu)}")
            chi = row.get('TT cho chủ nhà', 0) + row.get('Cọc cho chủ nhà', 0)
            if chi > 0: details.append(f"Chi:{fmt_vnd(chi)}")
            
            if not details: return "Trống"
            return ", ".join(details)

        df['_chi_tiet_nhap'] = df.apply(tao_mo_ta_dong, axis=1)

        agg_rules = {
            'Ngày ký': 'min', 'Ngày hết HĐ': 'max',
            'Ngày in': 'min', 'Ngày out': 'max',
            'Giá HĐ': 'max', 'Giá': 'max', 
            'TT cho chủ nhà': 'sum', 'Cọc cho chủ nhà': 'sum',
            'KH thanh toán': 'sum', 'KH cọc': 'sum',
            'Công ty': 'sum', 'Cá Nhân': 'sum',
            'SALE THẢO': 'sum', 'SALE NGA': 'sum', 'SALE LINH': 'sum',
            'Tên khách thuê': 'first',
            'Chủ nhà - sale': 'first',
            '_chi_tiet_nhap': lambda x: '\n'.join([f"• Lần {i+1}: {v}" for i, v in enumerate(x) if v != "Trống"])
        }
        
        final_agg = {k: v for k, v in agg_rules.items() if k in df.columns}
        cols_group = ['Toà', 'Mã căn']
        if not all(col in df.columns for col in cols_group): return df

        df_grouped = df.groupby(cols_group, as_index=False).agg(final_agg)
        df_grouped = df_grouped.rename(columns={'_chi_tiet_nhap': 'Ghi chú'})
        return df_grouped

    # ==============================================================================
    # 4. TẢI VÀ CHUẨN HÓA DỮ LIỆU (ĐÃ FIX LỖI MERGE)
    # ==============================================================================
    df_main = load_data("HOP_DONG")
    df_cp = load_data("CHI_PHI")

    # --- Clean Chi Phí ---
    if df_cp.empty:
        df_cp = pd.DataFrame(columns=COLUMNS_CP)
    else:
        df_cp.columns = df_cp.columns.str.strip()
        # ÉP BUỘC MÃ CĂN LÀ CHUỖI (Ngăn lỗi merge int64 và object)
        if "Mã căn" in df_cp.columns: 
            df_cp["Mã căn"] = df_cp["Mã căn"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        if "Ngày" in df_cp.columns: df_cp["Ngày"] = pd.to_datetime(df_cp["Ngày"], errors='coerce')
        if "Tiền" in df_cp.columns: df_cp["Tiền"] = df_cp["Tiền"].apply(clean_money)

    # --- Clean Hợp Đồng ---
    if not df_main.empty:
        df_main.columns = df_main.columns.str.strip()
        # ÉP BUỘC MÃ CĂN LÀ CHUỖI (Ngăn lỗi merge int64 và object)
        if "Mã căn" in df_main.columns: 
            df_main["Mã căn"] = df_main["Mã căn"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        for c in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
            if c in df_main.columns: df_main[c] = pd.to_datetime(df_main[c], errors='coerce')
        for c in COLS_MONEY:
            if c in df_main.columns: df_main[c] = df_main[c].apply(clean_money)

    # ==============================================================================
    # 5. SIDEBAR: THÔNG BÁO TÓM TẮT
    # ==============================================================================
    with st.sidebar:
        st.divider()
        st.header("🔔 Tóm tắt Thông Báo")
        today = pd.Timestamp(date.today())
        if not df_main.empty:
            df_alert_base = gop_du_lieu_phong(df_main)
            
            df_hd = df_alert_base[(df_alert_base['Ngày hết HĐ'].notna()) & ((df_alert_base['Ngày hết HĐ'] - today).dt.days.between(-999, 30))]
            df_kh = df_alert_base[(df_alert_base['Ngày out'].notna()) & ((df_alert_base['Ngày out'] - today).dt.days.between(0, 7))]

            if df_hd.empty and df_kh.empty: st.success("✅ Ổn định")
            else:
                if not df_hd.empty:
                    st.error(f"🔴 {len(df_hd)} HĐ cần xử lý")
                    for _, r in df_hd.iterrows():
                         days_left = (r['Ngày hết HĐ'] - today).days
                         status_msg = "ĐÃ HẾT HẠN" if days_left < 0 else f"Còn {days_left} ngày"
                         toa_nha = str(r.get('Toà', 'Chưa rõ')).strip()
                         st.markdown(f"**🏠 P.{r['Mã căn']}** ({toa_nha}) - {status_msg}")
                if not df_kh.empty:
                    st.warning(f"🟡 {len(df_kh)} Khách sắp out")
                    for _, r in df_kh.iterrows(): 
                        days_left = (r['Ngày out'] - today).days
                        toa_nha = str(r.get('Toà', 'Chưa rõ')).strip()
                        st.markdown(f"**🚪 P.{r['Mã căn']}** ({toa_nha}) - Còn {days_left} ngày")
        
        st.info("👉 Vào Tab **Cảnh Báo** để xem chi tiết và lấy mẫu tin nhắn.")
        st.divider()
        if st.button("🔄 Tải lại dữ liệu", use_container_width=True): 
            st.cache_data.clear()
            st.rerun()

    DANH_SACH_NHA = { "Tòa A": ["A101"], "Tòa B": ["B101"], "Khác": [] }

    # ==============================================================================
    # 6. GIAO DIỆN CHÍNH (TABS)
    # ==============================================================================
    tabs = st.tabs([
        "✍️ Nhập Liệu", "📥 Upload Excel", "💸 Chi Phí Nội Bộ", 
        "📋 Dữ Liệu Gốc", "🏠 Cảnh Báo", 
        "💰 Quản Lý Hợp Đồng", "📊 Lợi Nhuận (All)", "💸 Dòng Tiền Tháng",
        "📅 Quyết Toán Thuế" 
    ])

    with tabs[0]:
        st.subheader("✍️ Nhập Liệu Hợp Đồng Mới")
        av = st.session_state.get('auto', {}) 
        with st.form("main_form"):
            c1, c2, c3, c4 = st.columns(4)
            with c1: chon_toa = st.selectbox("Tòa nhà", list(DANH_SACH_NHA.keys()))
            with c2: chon_can = st.text_input("Mã căn", value=str(av.get("ma_can","")))
            with c3: chu_nha_sale = st.text_input("Chủ nhà - Sale")
            with c4: gia_thue = st.number_input("Giá thuê khách trả", step=100000, value=int(av.get("gia_thue", 0) or 0))
            c21, c22, c23, c24 = st.columns(4)