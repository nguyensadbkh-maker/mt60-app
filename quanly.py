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
# 2. KẾT NỐI DỮ LIỆU THÔNG MINH (BẢO MẬT STREAMLIT SECRETS)
# ==============================================================================

st.title("☁️ MT60 STUDIO - QUẢN LÝ TỔNG QUAN")
st.markdown("---")

st.sidebar.header("🔐 Trạng thái hệ thống")

@st.cache_resource
def connect_google_sheet(uploaded_file=None):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_dict = None
        
        if "google_credentials" in st.secrets:
            creds_dict = json.loads(st.secrets["google_credentials"])
        elif os.path.exists("key.json"):
            with open("key.json", "r", encoding="utf-8") as f:
                creds_dict = json.load(f)
        elif uploaded_file is not None:
            file_content = uploaded_file.read().decode("utf-8")
            creds_dict = json.loads(file_content)
            
        if creds_dict:
            if 'private_key' in creds_dict:
                creds_dict['private_key'] = creds_dict['private_key'].replace('\\\\n', '\n').replace('\\n', '\n')
            
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            return client.open(SHEET_NAME)
        return None
    except Exception as e:
        st.error(f"❌ Lỗi kết nối. Vui lòng kiểm tra lại file JSON hoặc Streamlit Secrets.")
        st.error(f"Chi tiết kỹ thuật: {e}")
        return None

# --- Khởi tạo kết nối ---
sh = None
if "google_credentials" in st.secrets or os.path.exists("key.json"):
    with st.spinner("Đang tự động kết nối hệ thống..."):
        sh = connect_google_sheet()
else:
    uploaded_key = st.sidebar.file_uploader("Vui lòng Upload file JSON gốc:", type=['json'])
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
    
    def clean_macan(col):
        return col.astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()

    def gop_du_lieu_phong(df_input):
        if df_input.empty: return df_input
        df = df_input.copy()
        df.columns = df.columns.str.strip()
        df['Mã căn'] = clean_macan(df['Mã căn'])

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
    # 4. TẢI VÀ CHUẨN HÓA DỮ LIỆU ĐẦU VÀO
    # ==============================================================================
    df_main = load_data("HOP_DONG")
    df_cp = load_data("CHI_PHI")

    if df_cp.empty:
        df_cp = pd.DataFrame(columns=COLUMNS_CP)
    else:
        df_cp.columns = df_cp.columns.str.strip()
        if "Mã căn" in df_cp.columns: df_cp["Mã căn"] = clean_macan(df_cp["Mã căn"])
        if "Ngày" in df_cp.columns: df_cp["Ngày"] = pd.to_datetime(df_cp["Ngày"], errors='coerce')
        if "Tiền" in df_cp.columns: df_cp["Tiền"] = df_cp["Tiền"].apply(clean_money)

    if not df_main.empty:
        df_main.columns = df_main.columns.str.strip()
        if "Mã căn" in df_main.columns: df_main["Mã căn"] = clean_macan(df_main["Mã căn"])
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

            def check_tenant_active(row):
                if pd.notna(row['Ngày in']) and pd.notna(row['Ngày out']):
                    return row['Ngày in'] <= today <= row['Ngày out']
                return False

            def check_owner_active(row):
                if pd.notna(row['Ngày ký']) and pd.notna(row['Ngày hết HĐ']):
                    return row['Ngày ký'] <= today <= row['Ngày hết HĐ']
                return False

            df_alert_base['has_tenant'] = df_alert_base.apply(check_tenant_active, axis=1)
            df_alert_base['has_owner'] = df_alert_base.apply(check_owner_active, axis=1)

            df_trong_co_hd = df_alert_base[(~df_alert_base['has_tenant']) & (df_alert_base['has_owner'])]
            df_trong_khong_hd = df_alert_base[(~df_alert_base['has_tenant']) & (~df_alert_base['has_owner'])]

            if df_hd.empty and df_kh.empty and df_trong_co_hd.empty and df_trong_khong_hd.empty: 
                st.success("✅ Ổn định. Lấp đầy 100%.")
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

                if not df_trong_co_hd.empty:
                    st.error(f"🔵 {len(df_trong_co_hd)} Trống - Đang gánh phí")
                    for _, r in df_trong_co_hd.iterrows(): 
                        toa_nha = str(r.get('Toà', 'Chưa rõ')).strip()
                        st.markdown(f"**🔴 P.{r['Mã căn']}** ({toa_nha})")

                if not df_trong_khong_hd.empty:
                    st.info(f"⚪ {len(df_trong_khong_hd)} Trống - Không HĐ chủ")
                    for _, r in df_trong_khong_hd.iterrows(): 
                        toa_nha = str(r.get('Toà', 'Chưa rõ')).strip()
                        st.markdown(f"**⚪ P.{r['Mã căn']}** ({toa_nha})")
        
        st.info("👉 Vào Tab **Cảnh Báo** để xem chi tiết.")
        st.divider()
        if st.button("🔄 Tải lại dữ liệu", use_container_width=True): 
            st.cache_data.clear()
            st.rerun()

    DANH_SACH_NHA = { "MT60": [], "MT61": [], "OC1A": [], "OC1B": [], "OC2A": [], "OC2B": [], "OC3": [] }

    # ==============================================================================
    # 6. GIAO DIỆN CHÍNH (TABS)
    # ==============================================================================
    tabs = st.tabs([
        "✍️ Nhập Liệu", "📥 Upload Excel", "💸 Chi Phí Nội Bộ", 
        "📋 Dữ Liệu Gốc", "🏠 Cảnh Báo", 
        "🏢 CP Hợp Đồng", "🏠 CP Cho Thuê",
        "💰 Quản Lý Tổng (Raw)",
        "📈 Theo dõi HĐKD" 
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
            with c21: ngay_ky = st.date_input("Ngày ký HĐ", date.today())
            with c22: 
                thoi_han = st.selectbox("Thời hạn", [6, 12, 1, 3, 24])
                try: ngay_het_hd = st.date_input("Ngày hết HĐ", value=ngay_ky + timedelta(days=thoi_han*30))
                except: ngay_het_hd = st.date_input("Ngày hết HĐ")
            with c23: ngay_in = st.date_input("Ngày in", ngay_ky)
            with c24: ngay_out = st.date_input("Ngày out", ngay_het_hd)
            c31, c32, c33, c34 = st.columns(4)
            with c31: ten_khach = st.text_input("Tên khách", value=str(av.get("ten_khach","")))
            with c32: gia_hd = st.number_input("Giá HĐ (Gốc)", step=100000)
            with c33: kh_coc = st.number_input("Khách cọc", step=100000)
            with c34: tt_chu_nha = st.number_input("TT cho chủ nhà", step=100000) 
            c41, c42, c43, c44 = st.columns(4)
            with c41: sale_thao = st.number_input("Sale Thảo", step=50000)
            with c42: sale_nga = st.number_input("Sale Nga", step=50000)
            with c43: sale_linh = st.number_input("Sale Linh", step=50000)
            with c44: cong_ty = st.number_input("Công ty", step=50000)
            
            if st.form_submit_button("💾 LƯU HỢP ĐỒNG", type="primary"):
                new_data = {"Tòa nhà": chon_toa, "Mã căn": chon_can, "Toà": chon_toa, "Chủ nhà - sale": chu_nha_sale, 
                            "Ngày ký": pd.to_datetime(ngay_ky), "Ngày hết HĐ": pd.to_datetime(ngay_het_hd), "Giá HĐ": gia_hd,
                            "TT cho chủ nhà": tt_chu_nha, "Tên khách thuê": ten_khach, "Ngày in": pd.to_datetime(ngay_in), "Ngày out": pd.to_datetime(ngay_out),
                            "Giá": gia_thue, "KH cọc": kh_coc, "Công ty": cong_ty, "SALE THẢO": sale_thao, "SALE NGA": sale_nga, "SALE LINH": sale_linh,
                            "Cọc cho chủ nhà": 0, "KH thanh toán": 0, "Cá Nhân": 0, "Hết hạn khách hàng": "", "Ráp khách khi hết hạn": ""}
                df_final = pd.concat([df_main, pd.DataFrame([new_data])], ignore_index=True)
                save_data(df_final, "HOP_DONG"); st.session_state['auto'] = {}; time.sleep(1); st.rerun()

    with tabs[1]:
        st.header("📤 Quản lý File Excel")
        st.download_button("📥 Tải File Mẫu", convert_df_to_excel(pd.DataFrame(columns=COLUMNS)), "mau_hop_dong.xlsx")
        up = st.file_uploader("Upload Excel", type=["xlsx"], key="up_main")
        if up and st.button("🚀 ĐỒNG BỘ CLOUD"):
            try:
                df_up = pd.read_excel(up)
                for col in COLS_MONEY:
                    if col in df_up.columns: df_up[col] = df_up[col].apply(clean_money)
                save_data(df_up, "HOP_DONG"); time.sleep(2); st.rerun()
            except Exception as e: st.error(f"Lỗi: {e}")

    with tabs[2]:
        st.subheader("💸 Chi Phí Nội Bộ")
        with st.form("cp_form"):
            c1, c2, c3, c4 = st.columns(4)
            d = c1.date_input("Ngày", date.today()); can = c2.text_input("Mã căn")
            loai = c3.selectbox("Loại", ["Điện", "Nước", "Net", "Dọn dẹp", "Khác"])
            tien = c4.number_input("Tiền", step=10000.0)
            if st.form_submit_button("Lưu"):
                new = pd.DataFrame([{"Mã căn": str(can).strip().upper(), "Loại": loai, "Tiền": tien, "Ngày": pd.to_datetime(d), "Chỉ số đồng hồ": ""}])
                save_data(pd.concat([df_cp, new], ignore_index=True), "CHI_PHI"); time.sleep(1); st.rerun()
        
        df_cp_show = df_cp.copy()
        df_cp_show["Tiền"] = df_cp_show["Tiền"].apply(fmt_vnd)
        st.dataframe(df_cp_show, use_container_width=True, column_config={"Ngày": st.column_config.DateColumn(format="DD/MM/YY")})

    with tabs[3]:
        st.subheader("📋 Dữ Liệu Gốc (Có thể Thêm/Xóa dòng)")
        st.info("💡 Để **XÓA DÒNG**, bạn hãy click vào cột ngoài cùng bên trái của dòng đó, rồi nhấn phím `Delete` trên bàn phím (hoặc biểu tượng thùng rác). Sau đó bấm **LƯU DỮ LIỆU GỐC**.")
        df_edit = df_main.copy()
        for c in COLS_MONEY:
            if c in df_edit.columns: 
                df_edit[c] = df_edit[c].apply(lambda x: str(int(x)) if pd.notna(x) else "0")
        
        edited_df = st.data_editor(
            df_edit, 
            use_container_width=True,
            num_rows="dynamic", 
            column_config={
                "Ngày ký": st.column_config.DateColumn(format="DD/MM/YY"),
                "Ngày hết HĐ": st.column_config.DateColumn(format="DD/MM/YY"),
                "Ngày in": st.column_config.DateColumn(format="DD/MM/YY"), 
                "Ngày out": st.column_config.DateColumn(format="DD/MM/YY"),
            }
        )
        if st.button("💾 LƯU DỮ LIỆU GỐC", type="primary"):
            df_to_save = edited_df.copy()
            for c in COLS_MONEY:
                if c in df_to_save.columns: df_to_save[c] = df_to_save[c].apply(clean_money)
            save_data(df_to_save, "HOP_DONG")
            time.sleep(1); st.rerun()

    with tabs[4]:
        st.subheader("🏠 Trung Tâm Cảnh Báo Chi Tiết")
        if not df_main.empty:
            df_alert_tab = gop_du_lieu_phong(df_main)
            today = pd.Timestamp(date.today())
            
            st.write("#### 1️⃣ Cảnh báo Hết Hạn Hợp Đồng (Với Chủ Nhà)")
            def check_hd(row):
                x = row['Ngày hết HĐ']
                if pd.isna(x): return "N/A"
                days = (x - today).days
                if days < 0: return "Hết hạn"
                if days <= 30: return "Sắp hết"
                return "Còn hạn"
            
            df_warning_hd = df_alert_tab[df_alert_tab.apply(lambda r: check_hd(r) in ["Hết hạn", "Sắp hết"], axis=1)]
            if df_warning_hd.empty: 
                st.success("✅ Không có HĐ sắp hết hạn.")
            else:
                for idx, row in df_warning_hd.iterrows():
                    days = (row['Ngày hết HĐ'] - today).days
                    status = "ĐÃ QUÁ HẠN" if days < 0 else f"Còn {days} ngày"
                    toa_nha = str(row.get('Toà', 'Chưa rõ')).strip()
                    chu_nha = str(row.get('Chủ nhà - sale', 'Chưa rõ'))
                    
                    with st.expander(f"🔴 Tòa {toa_nha} - P.{row['Mã căn']} ({status})"):
                        c1, c2, c3 = st.columns(3)
                        c1.markdown(f"**Chủ nhà/Sale:** {chu_nha}")
                        c2.markdown(f"**Giá HĐ:** {fmt_vnd(row.get('Giá HĐ', 0))}")
                        c3.markdown(f"**Hết HĐ:** {fmt_date(row['Ngày hết HĐ'])}")
                        
                        st.markdown("📝 **Mẫu tin nhắn làm việc với Chủ nhà:**")
                        st.code(f"Chào anh/chị {chu_nha},\nHợp đồng thuê phòng {row['Mã căn']} tòa {toa_nha} sẽ hết hạn vào ngày {fmt_date(row['Ngày hết HĐ'])}.\nBQL xin phép liên hệ anh/chị để trao đổi về việc gia hạn hợp đồng ạ.", language="text")

            st.divider()
            
            st.write("#### 2️⃣ Cảnh báo Khách Sắp Trả Phòng (Check-out)")
            def check_out(row):
                x = row['Ngày out']
                if pd.isna(x): return "N/A"
                days = (x - today).days
                if 0 <= days <= 7: return "Sắp out"
                return "Còn ở"
            
            df_warning_out = df_alert_tab[df_alert_tab.apply(lambda r: check_out(r) == "Sắp out", axis=1)]
            if df_warning_out.empty: 
                st.success("✅ Không có phòng sắp trả.")
            else:
                for idx, row in df_warning_out.iterrows():
                    days = (row['Ngày out'] - today).days
                    toa_nha = str(row.get('Toà', 'Chưa rõ')).strip()
                    khach = str(row.get('Tên khách thuê', 'Khách'))
                    coc = row.get('KH cọc', 0)
                    
                    with st.expander(f"🚪 Tòa {toa_nha} - P.{row['Mã căn']} - Khách: {khach} (Còn {days} ngày)"):
                        c1, c2, c3 = st.columns(3)
                        c1.markdown(f"**Khách thuê:** {khach}")
                        c2.markdown(f"**Giá thuê:** {fmt_vnd(row.get('Giá', 0))}")
                        c3.markdown(f"**Tiền cọc hoàn trả:** {fmt_vnd(coc)}")
                        
                        c4, c5, _ = st.columns(3)
                        c4.markdown(f"**Ngày vào:** {fmt_date(row['Ngày in'])}")
                        c5.markdown(f"**Ngày ra:** {fmt_date(row['Ngày out'])}")
                        
                        st.markdown("📝 **Mẫu tin nhắn nhắc khách:**")
                        st.code(f"Chào {khach},\nPhòng {row['Mã căn']} tòa {toa_nha} của bạn sẽ đến hạn trả phòng vào ngày {fmt_date(row['Ngày out'])}.\nBạn vui lòng chuẩn bị dọn dẹp và liên hệ BQL để chốt số điện nước, làm thủ tục bàn giao và hoàn cọc ({fmt_vnd(coc)}) nhé. Cảm ơn bạn!", language="text")

            st.divider()

            df_alert_tab['has_tenant_4'] = df_alert_tab.apply(lambda r: (r['Ngày in'] <= today <= r['Ngày out']) if pd.notna(r['Ngày in']) and pd.notna(r['Ngày out']) else False, axis=1)
            df_alert_tab['has_owner_4'] = df_alert_tab.apply(lambda r: (r['Ngày ký'] <= today <= r['Ngày hết HĐ']) if pd.notna(r['Ngày ký']) and pd.notna(r['Ngày hết HĐ']) else False, axis=1)

            df_tab_trong_co_hd = df_alert_tab[(~df_alert_tab['has_tenant_4']) & (df_alert_tab['has_owner_4'])]
            df_tab_trong_khong_hd = df_alert_tab[(~df_alert_tab['has_tenant_4']) & (~df_alert_tab['has_owner_4'])]

            st.write("#### 3️⃣ Cảnh báo Phòng Trống - ĐANG GÁNH PHÍ (Có HĐ Chủ)")
            if df_tab_trong_co_hd.empty:
                st.success("✅ Tuyệt vời! Không có phòng nào đang trống mà phải gánh phí chủ nhà.")
            else:
                for idx, row in df_tab_trong_co_hd.iterrows():
                    toa_nha = str(row.get('Toà', 'Chưa rõ')).strip()
                    chu_nha = str(row.get('Chủ nhà - sale', 'Chưa rõ'))
                    gia_hd = row.get('Giá HĐ', 0)
                    
                    with st.expander(f"🔴 Tòa {toa_nha} - P.{row['Mã căn']} (Đang rớt tiền)"):
                        c1, c2 = st.columns(2)
                        c1.markdown(f"**Chủ nhà/Sale:** {chu_nha}")
                        c2.markdown(f"**Giá vốn đang gánh:** {fmt_vnd(gia_hd)}")
                        
                        st.markdown("📝 **Mẫu tin nhắn Push Sale:**")
                        st.code(f"🚨 SOS: Phòng {row['Mã căn']} tòa {toa_nha} hiện đang trống và đang phải gánh phí chủ nhà ({fmt_vnd(gia_hd)}). ACE tập trung push khách chốt lấp đầy ngay giúp quản lý nhé!", language="text")

            st.divider()

            st.write("#### 4️⃣ Danh sách Phòng Trống - THUẦN (Không có HĐ Chủ)")
            if df_tab_trong_khong_hd.empty:
                st.info("Hiện không có quỹ phòng trống dự trữ.")
            else:
                for idx, row in df_tab_trong_khong_hd.iterrows():
                    toa_nha = str(row.get('Toà', 'Chưa rõ')).strip()
                    
                    with st.expander(f"⚪ Tòa {toa_nha} - P.{row['Mã căn']} (Trống nhàn rỗi)"):
                        st.markdown("Phòng này hiện tại không có khách thuê và cũng chưa ký (hoặc đã hết hạn) hợp đồng với chủ nhà. Không phát sinh chi phí.")
                        st.markdown("📝 **Mẫu tin nhắn Sale:**")
                        st.code(f"Phòng {row['Mã căn']} tòa {toa_nha} hiện đang sẵn sàng để ký mới. ACE có khách báo lại BQL để làm việc với chủ nhà chốt giá nhé.", language="text")

    with tabs[5]:
        st.subheader("🏢 Quản Lý Chi Phí Hợp Đồng (Trả Chủ Nhà)")
        col1, col2 = st.columns(2)
        with col1: m_hd = st.selectbox("Chọn Tháng", range(1, 13), index=date.today().month - 1, key='m_hd')
        with col2: y_hd = st.number_input("Chọn Năm", value=date.today().year, key='y_hd')
        st.divider()

        start_mo_hd = pd.Timestamp(y_hd, m_hd, 1)
        if m_hd == 12: end_mo_hd = pd.Timestamp(y_hd + 1, 1, 1) - pd.Timedelta(days=1)
        else: end_mo_hd = pd.Timestamp(y_hd, m_hd + 1, 1) - pd.Timedelta(days=1)

        if not df_main.empty:
            df_raw_hd = df_main.copy()
            
            def process_row_hd(row):
                hd_active = False
                if pd.notna(row['Ngày ký']) and pd.notna(row['Ngày hết HĐ']):
                    if row['Ngày ký'] <= end_mo_hd and row['Ngày hết HĐ'] >= start_mo_hd: 
                        hd_active = True
                
                if not hd_active or row.get('Giá HĐ', 0) <= 0:
                    return pd.Series([False, "", "", "", 0, 0], 
                                     index=['_keep', 'Thời hạn HĐ', 'Trạng thái', 'Thời hạn cho thuê', 'Giá thuê', 'Lợi nhuận ròng'])

                thoi_han_hd = f"{fmt_date(row['Ngày ký'])} - {fmt_date(row['Ngày hết HĐ'])}"

                tenant_active = False
                if pd.notna(row['Ngày in']) and pd.notna(row['Ngày out']):
                    if row['Ngày in'] <= end_mo_hd and row['Ngày out'] >= start_mo_hd:
                        tenant_active = True

                if tenant_active:
                    trang_thai = "Đã có khách thuê"
                    thoi_han_thue = f"{fmt_date(row['Ngày in'])} - {fmt_date(row['Ngày out'])}"
                    gia_thue = row.get('Giá', 0)
                else:
                    trang_thai = "Trống"
                    thoi_han_thue = "N/A"
                    gia_thue = 0

                loi_nhuan = gia_thue - row.get('Giá HĐ', 0)

                return pd.Series([True, thoi_han_hd, trang_thai, thoi_han_thue, gia_thue, loi_nhuan], 
                                 index=['_keep', 'Thời hạn HĐ', 'Trạng thái', 'Thời hạn cho thuê', 'Giá thuê', 'Lợi nhuận ròng'])

            hd_calcs = df_raw_hd.apply(process_row_hd, axis=1)
            df_view_hd = pd.concat([df_raw_hd, hd_calcs], axis=1)
            df_view_hd = df_view_hd[df_view_hd['_keep'] == True]
            
            if not df_view_hd.empty:
                df_view_hd = df_view_hd.sort_values(by=['Giá thuê'], ascending=False)
                df_view_hd = df_view_hd.drop_duplicates(subset=['Toà', 'Mã căn', 'Thời hạn HĐ'], keep='first')
                df_view_hd = df_view_hd.sort_values(by=['Toà', 'Mã căn'])

                st.write(f"#### 📊 Tổng hợp chi phí Hợp Đồng tháng {m_hd}/{y_hd}")
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Tổng Giá HĐ (Chủ nhà)", fmt_vnd(df_view_hd['Giá HĐ'].sum()))
                m2.metric("Tổng TT Chủ Nhà", fmt_vnd(df_view_hd['TT cho chủ nhà'].sum()))
                m3.metric("Tổng Cọc Chủ Nhà", fmt_vnd(df_view_hd['Cọc cho chủ nhà'].sum()))
                m4.metric("Tổng Giá Thuê (Khách)", fmt_vnd(df_view_hd['Giá thuê'].sum()))
                m5.metric("Tổng Lợi Nhuận Ròng", fmt_vnd(df_view_hd['Lợi nhuận ròng'].sum()))
                st.markdown("---")

                cols_show = [
                    "Toà", "Mã căn", "Chủ nhà - sale", "Thời hạn HĐ", "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà",
                    "Trạng thái", "Thời hạn cho thuê", "Giá thuê", "Lợi nhuận ròng"
                ]
                cols_exist = [c for c in cols_show if c in df_view_hd.columns]
                df_display_hd = df_view_hd[cols_exist].copy()
                df_export_hd = df_display_hd.copy() 
                
                num_cols = ["Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Giá thuê", "Lợi nhuận ròng"]
                for c in num_cols: 
                    if c in df_display_hd.columns: 
                        df_display_hd[c] = df_display_hd[c].apply(fmt_vnd)
                
                def color_negative_red(val):
                    color = 'red' if isinstance(val, str) and '(' in val else 'black'
                    return f'color: {color}'
                
                styler = df_display_hd.style.applymap(color_negative_red, subset=['Lợi nhuận ròng']).set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'})
                st.dataframe(styler, use_container_width=True)
                st.download_button("📥 Tải Excel CPHĐ", convert_df_to_excel(df_export_hd), f"CP_HopDong_{m_hd}_{y_hd}.xlsx")
            else:
                st.warning(f"Không có căn nào có Giá HĐ > 0 hoạt động trong tháng {m_hd}/{y_hd}")

    with tabs[6]:
        st.subheader("🏠 Quản Lý Chi Phí Cho Thuê (Thu Khách Hàng)")
        col1, col2 = st.columns(2)
        with col1: m_ct = st.selectbox("Chọn Tháng", range(1, 13), index=date.today().month - 1, key='m_ct')
        with col2: y_ct = st.number_input("Chọn Năm", value=date.today().year, key='y_ct')
        st.divider()

        start_mo_ct = pd.Timestamp(y_ct, m_ct, 1)
        if m_ct == 12: end_mo_ct = pd.Timestamp(y_ct + 1, 1, 1) - pd.Timedelta(days=1)
        else: end_mo_ct = pd.Timestamp(y_ct, m_ct + 1, 1) - pd.Timedelta(days=1)

        if not df_main.empty:
            df_raw_ct = df_main.copy()
            
            def process_row_ct(row):
                tenant_active = False
                if pd.notna(row['Ngày in']) and pd.notna(row['Ngày out']):
                    if row['Ngày in'] <= end_mo_ct and row['Ngày out'] >= start_mo_ct: 
                        tenant_active = True
                
                if not tenant_active or row.get('Giá', 0) <= 0:
                    return pd.Series([False, "", "", "", 0, 0], 
                                     index=['_keep', 'Thời hạn cho thuê', 'Trạng thái HĐ Chủ', 'Thời hạn HĐ', 'Giá HĐ Chủ', 'Lợi nhuận ròng'])

                thoi_han_thue = f"{fmt_date(row['Ngày in'])} - {fmt_date(row['Ngày out'])}"
                gia_thue = row.get('Giá', 0)

                hd_active = False
                if pd.notna(row['Ngày ký']) and pd.notna(row['Ngày hết HĐ']):
                    if row['Ngày ký'] <= end_mo_ct and row['Ngày hết HĐ'] >= start_mo_ct:
                        hd_active = True

                if hd_active:
                    trang_thai_chu = "Đã có HĐ Chủ"
                    thoi_han_hd = f"{fmt_date(row['Ngày ký'])} - {fmt_date(row['Ngày hết HĐ'])}"
                    gia_hd = row.get('Giá HĐ', 0)
                else:
                    trang_thai_chu = "Trống HĐ Gốc"
                    thoi_han_hd = "N/A"
                    gia_hd = 0

                loi_nhuan = gia_thue - gia_hd

                return pd.Series([True, thoi_han_thue, trang_thai_chu, thoi_han_hd, gia_hd, loi_nhuan], 
                                 index=['_keep', 'Thời hạn cho thuê', 'Trạng thái HĐ Chủ', 'Thời hạn HĐ', 'Giá HĐ Chủ', 'Lợi nhuận ròng'])

            ct_calcs = df_raw_ct.apply(process_row_ct, axis=1)
            df_view_ct = pd.concat([df_raw_ct, ct_calcs], axis=1)
            df_view_ct = df_view_ct[df_view_ct['_keep'] == True]
            
            if not df_view_ct.empty:
                df_view_ct = df_view_ct.sort_values(by=['Giá HĐ Chủ'], ascending=False)
                df_view_ct = df_view_ct.drop_duplicates(subset=['Toà', 'Mã căn', 'Thời hạn cho thuê'], keep='first')
                df_view_ct = df_view_ct.sort_values(by=['Toà', 'Mã căn'])

                df_da_co = df_view_ct[df_view_ct['Trạng thái HĐ Chủ'] == "Đã có HĐ Chủ"]
                df_trong = df_view_ct[df_view_ct['Trạng thái HĐ Chủ'] == "Trống HĐ Gốc"]

                st.write(f"#### 📊 [Nhóm 1] Đã có Hợp đồng với Chủ nhà")
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Tổng Giá Thuê", fmt_vnd(df_da_co['Giá'].sum()))
                m2.metric("Tổng KH Thanh Toán", fmt_vnd(df_da_co['KH thanh toán'].sum()))
                m3.metric("Tổng KH Cọc", fmt_vnd(df_da_co['KH cọc'].sum()))
                m4.metric("Tổng Giá HĐ Chủ", fmt_vnd(df_da_co['Giá HĐ Chủ'].sum()))
                m5.metric("Tổng Lợi Nhuận Ròng", fmt_vnd(df_da_co['Lợi nhuận ròng'].sum()))

                st.write(f"#### 📊 [Nhóm 2] Trống Hợp đồng gốc (Thuần lãi)")
                n1, n2, n3, n4, n5 = st.columns(5)
                n1.metric("Tổng Giá Thuê", fmt_vnd(df_trong['Giá'].sum()))
                n2.metric("Tổng KH Thanh Toán", fmt_vnd(df_trong['KH thanh toán'].sum()))
                n3.metric("Tổng KH Cọc", fmt_vnd(df_trong['KH cọc'].sum()))
                n4.metric("Tổng Giá HĐ Chủ", fmt_vnd(df_trong['Giá HĐ Chủ'].sum())) 
                n5.metric("Tổng Lợi Nhuận Ròng", fmt_vnd(df_trong['Lợi nhuận ròng'].sum()))
                st.markdown("---")

                cols_show = [
                    "Toà", "Mã căn", "Tên khách thuê", "Thời hạn cho thuê", "Giá", "KH thanh toán", "KH cọc",
                    "Trạng thái HĐ Chủ", "Thời hạn HĐ", "Giá HĐ Chủ", "Lợi nhuận ròng"
                ]
                cols_exist = [c for c in cols_show if c in df_view_ct.columns]
                df_display_ct = df_view_ct[cols_exist].copy()
                
                df_display_ct = df_display_ct.rename(columns={'Giá': 'Giá thuê', 'Giá HĐ Chủ': 'Giá HĐ'})
                df_export_ct = df_display_ct.copy() 
                
                num_cols = ["Giá thuê", "KH thanh toán", "KH cọc", "Giá HĐ", "Lợi nhuận ròng"]
                for c in num_cols: 
                    if c in df_display_ct.columns: 
                        df_display_ct[c] = df_display_ct[c].apply(fmt_vnd)
                
                def color_negative_red(val):
                    color = 'red' if isinstance(val, str) and '(' in val else 'black'
                    return f'color: {color}'
                
                styler = df_display_ct.style.applymap(color_negative_red, subset=['Lợi nhuận ròng']).set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'})
                st.dataframe(styler, use_container_width=True)
                st.download_button("📥 Tải Excel Khách Thuê", convert_df_to_excel(df_export_ct), f"CP_ChoThue_{m_ct}_{y_ct}.xlsx")
            else:
                st.warning(f"Không có căn nào có Giá thuê > 0 hoạt động trong tháng {m_ct}/{y_ct}")

    with tabs[7]:
        st.subheader("💰 Quản Lý Tổng Hợp (Lọc theo Tháng - Không gộp dòng)")
        col1, col2 = st.columns(2)
        with col1: m_chung = st.selectbox("Chọn Tháng", range(1, 13), index=date.today().month - 1, key='m_chung')
        with col2: y_chung = st.number_input("Chọn Năm", value=date.today().year, key='y_chung')
        st.divider()

        start_mo_chung = pd.Timestamp(y_chung, m_chung, 1)
        if m_chung == 12: end_mo_chung = pd.Timestamp(y_chung + 1, 1, 1) - pd.Timedelta(days=1)
        else: end_mo_chung = pd.Timestamp(y_chung, m_chung + 1, 1) - pd.Timedelta(days=1)

        if not df_main.empty:
            df_raw_chung = df_main.copy()

            def is_active_chung(row):
                hd_active = False
                if pd.notna(row['Ngày ký']) and pd.notna(row['Ngày hết HĐ']):
                    if row['Ngày ký'] <= end_mo_chung and row['Ngày hết HĐ'] >= start_mo_chung:
                        hd_active = True

                tenant_active = False
                if pd.notna(row['Ngày in']) and pd.notna(row['Ngày out']):
                    if row['Ngày in'] <= end_mo_chung and row['Ngày out'] >= start_mo_chung:
                        tenant_active = True

                return hd_active or tenant_active

            df_view_chung = df_raw_chung[df_raw_chung.apply(is_active_chung, axis=1)].copy()

            if not df_view_chung.empty:
                df_view_chung = df_view_chung.sort_values(by=['Toà', 'Mã căn'])

                df_view_chung['Ngày ký'] = df_view_chung['Ngày ký'].apply(fmt_date)
                df_view_chung['Ngày hết HĐ'] = df_view_chung['Ngày hết HĐ'].apply(fmt_date)
                df_view_chung['Ngày in'] = df_view_chung['Ngày in'].apply(fmt_date)
                df_view_chung['Ngày out'] = df_view_chung['Ngày out'].apply(fmt_date)

                cols_show = [
                    "Toà", "Mã căn", "Chủ nhà - sale", "Ngày ký", "Ngày hết HĐ", "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà",
                    "Tên khách thuê", "Ngày in", "Ngày out", "Giá", "KH thanh toán", "KH cọc"
                ]
                cols_exist = [c for c in cols_show if c in df_view_chung.columns]
                df_display_chung = df_view_chung[cols_exist].copy()
                df_export_chung = df_display_chung.copy()

                num_cols = ["Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Giá", "KH thanh toán", "KH cọc"]
                for c in num_cols:
                    if c in df_display_chung.columns:
                        df_display_chung[c] = df_display_chung[c].apply(fmt_vnd)

                st.dataframe(df_display_chung.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}), use_container_width=True)
                st.download_button("📥 Tải Excel", convert_df_to_excel(df_export_chung), f"QuanLy_TongHop_{m_chung}_{y_chung}.xlsx")
            else:
                st.warning(f"Không có dữ liệu hoạt động trong tháng {m_chung}/{y_chung}")

    # --- TAB 8: THEO DÕI HĐKD VÀ GIẢI TRÌNH CHI TIẾT ---
    with tabs[8]:
        st.subheader("📈 Theo Dõi Hoạt Động Kinh Doanh")
        st.write("Báo cáo tự động tính toán dòng tiền thu - chi - lợi nhuận. Bạn có thể mở từng tháng để xem giải trình chi tiết từng phòng.")
        
        current_year = date.today().year
        current_month = date.today().month

        y_kd = st.selectbox("Chọn Năm Tài Chính", range(2020, current_year + 5), index=(current_year - 2020), key='y_kd')
        st.divider()

        max_month = 12
        if y_kd == current_year:
            max_month = current_month
        elif y_kd > current_year:
            max_month = 0

        # Hàm tính toán và TRẢ VỀ CÁC BẢNG DATA ĐỂ GIẢI TRÌNH
        def calc_month_stats_detailed(df_raw, df_chiphi, month, year):
            start_d = pd.Timestamp(year, month, 1)
            if month == 12: end_d = pd.Timestamp(year + 1, 1, 1) - pd.Timedelta(days=1)
            else: end_d = pd.Timestamp(year, month + 1, 1) - pd.Timedelta(days=1)

            dt_co_hd = 0
            dt_khong_hd = 0
            chi_phi_hd = 0
            chi_phi_vh = 0

            df_dt_co = pd.DataFrame()
            df_dt_khong = pd.DataFrame()
            df_hd_cost = pd.DataFrame()
            df_cp_vh = pd.DataFrame()

            if not df_raw.empty:
                # 1. CHI PHÍ CHỦ NHÀ
                df_hd = df_raw.copy()
                df_hd['owner_active'] = df_hd.apply(lambda r: True if pd.notna(r['Ngày ký']) and pd.notna(r['Ngày hết HĐ']) and r['Ngày ký'] <= end_d and r['Ngày hết HĐ'] >= start_d else False, axis=1)
                
                df_hd_active = df_hd[df_hd['owner_active']]
                active_owner_tuples = set(zip(df_hd_active['Toà'], df_hd_active['Mã căn']))

                df_hd_c = df_hd_active[df_hd_active['Giá HĐ'] > 0].copy()
                if not df_hd_c.empty:
                    df_hd_c['Thời hạn HĐ'] = df_hd_c['Ngày ký'].apply(fmt_date) + " - " + df_hd_c['Ngày hết HĐ'].apply(fmt_date)
                    df_hd_c = df_hd_c.sort_values(by=['Giá HĐ'], ascending=False) 
                    df_hd_cost = df_hd_c.drop_duplicates(subset=['Toà', 'Mã căn', 'Thời hạn HĐ'], keep='first')
                    chi_phi_hd = df_hd_cost['Giá HĐ'].sum()

                # 2. DOANH THU KHÁCH
                df_ct = df_raw.copy()
                df_ct['tenant_active'] = df_ct.apply(lambda r: True if pd.notna(r['Ngày in']) and pd.notna(r['Ngày out']) and r['Ngày in'] <= end_d and r['Ngày out'] >= start_d else False, axis=1)
                df_ct = df_ct[df_ct['tenant_active'] & (df_ct['Giá'] > 0)].copy()
                
                if not df_ct.empty:
                    df_ct['Thời hạn cho thuê'] = df_ct['Ngày in'].apply(fmt_date) + " - " + df_ct['Ngày out'].apply(fmt_date)
                    df_ct = df_ct.sort_values(by=['Giá'], ascending=False)
                    df_ct = df_ct.drop_duplicates(subset=['Toà', 'Mã căn', 'Thời hạn cho thuê'], keep='first')
                    
                    is_co_hd = df_ct.apply(lambda r: (r['Toà'], r['Mã căn']) in active_owner_tuples, axis=1)
                    df_dt_co = df_ct[is_co_hd]
                    df_dt_khong = df_ct[~is_co_hd]

                    dt_co_hd = df_dt_co['Giá'].sum()
                    dt_khong_hd = df_dt_khong['Giá'].sum()

            # 3. CHI PHÍ VẬN HÀNH
            if not df_chiphi.empty:
                mask_cp = (df_chiphi['Ngày'] >= start_d) & (df_chiphi['Ngày'] <= end_d)
                df_cp_vh = df_chiphi[mask_cp].copy()
                chi_phi_vh = pd.to_numeric(df_cp_vh['Tiền'], errors='coerce').sum()

            loi_nhuan = dt_co_hd - chi_phi_hd - chi_phi_vh
            return dt_co_hd, dt_khong_hd, chi_phi_hd, chi_phi_vh, loi_nhuan, df_dt_co, df_dt_khong, df_hd_cost, df_cp_vh

        if not df_main.empty and max_month > 0:
            yearly_data = []
            detailed_data = {}

            # Chạy vòng lặp tính toán và lưu bảng chi tiết
            for m in range(1, max_month + 1):
                dt_co, dt_khong, cp_hd, cp_vh, ln, d_dt_co, d_dt_khong, d_hd_cost, d_cp_vh = calc_month_stats_detailed(df_main, df_cp, m, y_kd)
                yearly_data.append({
                    "Tháng": f"Tháng {m}",
                    "Doanh Thu (Có HĐ gốc)": dt_co,
                    "Chi Phí HĐ (Chủ nhà)": cp_hd,
                    "Chi Phí Khác (VH)": cp_vh,
                    "Lợi Nhuận Ròng": ln,
                    "DT Treo (Không HĐ)": dt_khong
                })
                detailed_data[m] = {
                    'dt_co': d_dt_co,
                    'dt_khong': d_dt_khong,
                    'cp_hd': d_hd_cost,
                    'cp_vh': d_cp_vh
                }
            
            df_year = pd.DataFrame(yearly_data)

            # HIỂN THỊ TỔNG QUAN
            st.write(f"### 🏆 BẢNG TỔNG KẾT ĐẾN THÁNG {max_month}/{y_kd}")
            t1, t2, t3, t4, t5 = st.columns(5)
            t1.metric("Doanh Thu (Có HĐ Gốc)", fmt_vnd(df_year["Doanh Thu (Có HĐ gốc)"].sum()))
            t2.metric("Chi Phí Trả Chủ Nhà", fmt_vnd(df_year["Chi Phí HĐ (Chủ nhà)"].sum()))
            t3.metric("Chi Phí Khác", fmt_vnd(df_year["Chi Phí Khác (VH)"].sum()))
            t4.metric("Lợi Nhuận Ròng", fmt_vnd(df_year["Lợi Nhuận Ròng"].sum()), delta_color="normal" if df_year["Lợi Nhuận Ròng"].sum() > 0 else "inverse")
            t5.metric("DT Treo (Không HĐ)", fmt_vnd(df_year["DT Treo (Không HĐ)"].sum()), delta_color="off")
            
            df_year_display = df_year.copy()
            for col in ["Doanh Thu (Có HĐ gốc)", "Chi Phí HĐ (Chủ nhà)", "Chi Phí Khác (VH)", "Lợi Nhuận Ròng", "DT Treo (Không HĐ)"]:
                df_year_display[col] = df_year_display[col].apply(fmt_vnd)
            
            def color_negative_red_year(val):
                color = 'red' if isinstance(val, str) and '(' in val else 'black'
                return f'color: {color}'

            st.dataframe(
                df_year_display.style.applymap(color_negative_red_year, subset=['Lợi Nhuận Ròng'])
                                     .set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}), 
                use_container_width=True
            )
            
            st.download_button("📥 Tải Bảng Báo Cáo Tổng Excel", convert_df_to_excel(df_year), f"BaoCao_KinhDoanh_{y_kd}.xlsx")
            st.divider()

            # HIỂN THỊ PHẦN GIẢI TRÌNH CHI TIẾT DƯỚI DẠNG EXPANDER
            st.write("#### 🔍 Giải trình chi tiết từng tháng")
            st.info("💡 Bấm vào từng tháng bên dưới để đối soát các phòng tạo ra Doanh thu và Chi phí.")
            
            for m in range(1, max_month + 1):
                with st.expander(f"📋 Mở xem chi tiết Tháng {m}/{y_kd}"):
                    d_m = detailed_data[m]
                    
                    t_hd, t_cp = st.tabs(["📊 Doanh Thu & Chi Phí HĐ", "🔌 Chi Phí Vận Hành"])
                    
                    with t_hd:
                        # 1. Doanh thu có HĐ
                        st.markdown("**🟢 DOANH THU CHÍNH THỨC (Các phòng đang có HĐ Chủ)**")
                        if not d_m['dt_co'].empty:
                            df_dt_co_disp = d_m['dt_co'][['Toà', 'Mã căn', 'Tên khách thuê', 'Giá']].copy()
                            df_dt_co_disp['Giá'] = df_dt_co_disp['Giá'].apply(fmt_vnd)
                            st.dataframe(df_dt_co_disp, use_container_width=True)
                        else:
                            st.caption("Không có dữ liệu trong tháng này.")
                            
                        # 2. Chi phí HĐ
                        st.markdown("**🔴 CHI PHÍ HỢP ĐỒNG (Tiền trả Chủ nhà)**")
                        if not d_m['cp_hd'].empty:
                            df_cp_hd_disp = d_m['cp_hd'][['Toà', 'Mã căn', 'Chủ nhà - sale', 'Giá HĐ']].copy()
                            df_cp_hd_disp['Giá HĐ'] = df_cp_hd_disp['Giá HĐ'].apply(fmt_vnd)
                            st.dataframe(df_cp_hd_disp, use_container_width=True)
                        else:
                            st.caption("Không có chi phí trả chủ nhà trong tháng này.")
                            
                        # 3. DT Treo
                        st.markdown("**⚪ DOANH THU TREO (Phòng có khách nhưng KHÔNG CÓ HĐ Chủ)**")
                        if not d_m['dt_khong'].empty:
                            df_dt_khong_disp = d_m['dt_khong'][['Toà', 'Mã căn', 'Tên khách thuê', 'Giá']].copy()
                            df_dt_khong_disp['Giá'] = df_dt_khong_disp['Giá'].apply(fmt_vnd)
                            st.dataframe(df_dt_khong_disp, use_container_width=True)
                        else:
                            st.caption("Không có khoản doanh thu treo nào.")
                            
                    with t_cp:
                        st.markdown("**🟠 CHI PHÍ VẬN HÀNH (Điện, nước, dọn dẹp...)**")
                        if not d_m['cp_vh'].empty:
                            df_cp_vh_disp = d_m['cp_vh'][['Ngày', 'Mã căn', 'Loại', 'Tiền']].copy()
                            df_cp_vh_disp['Tiền'] = df_cp_vh_disp['Tiền'].apply(fmt_vnd)
                            if pd.api.types.is_datetime64_any_dtype(df_cp_vh_disp['Ngày']):
                                df_cp_vh_disp['Ngày'] = df_cp_vh_disp['Ngày'].dt.strftime('%d/%m/%Y')
                            st.dataframe(df_cp_vh_disp, use_container_width=True)
                        else:
                            st.caption("Không có chi phí phát sinh trong tháng này.")

        elif max_month == 0:
            st.warning("Chưa có dữ liệu hoạt động cho năm tương lai.")