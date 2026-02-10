import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
import os
import json
import re
import time
import io
from PIL import Image

# --- THƯ VIỆN KẾT NỐI GOOGLE SHEETS ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
st.set_page_config(
    page_title="MT60 Cloud Manager", 
    layout="wide", 
    page_icon="☁️",
    initial_sidebar_state="expanded"
)

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

# ==========================================
# 2. GIAO DIỆN & KẾT NỐI
# ==========================================
st.title("☁️ MT60 STUDIO - HỆ THỐNG QUẢN LÝ TOÀN DIỆN")
st.markdown("---")

st.sidebar.header("🔐 Đăng Nhập Hệ Thống")
uploaded_key = st.sidebar.file_uploader("Chọn file JSON (Chìa khóa) để mở khóa", type=['json'])

@st.cache_resource
def connect_google_sheet(file_obj):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        file_content = file_obj.read().decode("utf-8")
        creds_dict = json.loads(file_content)
        if 'private_key' in creds_dict:
             creds_dict['private_key'] = creds_dict['private_key'].replace('\\n', '\n')
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sh = client.open(SHEET_NAME)
        return sh
    except Exception as e:
        st.error(f"❌ Lỗi kết nối: {e}")
        return None

# ==========================================
# 3. XỬ LÝ DỮ LIỆU CHÍNH
# ==========================================
if uploaded_key is not None:
    uploaded_key.seek(0)
    with st.spinner("Đang kết nối đến máy chủ Google..."):
        sh = connect_google_sheet(uploaded_key)
    
    if sh:
        st.sidebar.success("✅ Đã kết nối thành công!")
        
        # --- CÁC HÀM HỖ TRỢ ---
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

        def to_num(val):
            if isinstance(val, str): 
                val = val.replace(',', '').replace('.', '').strip()
                if val == '' or val.lower() == 'nan': return 0
            try: return float(val)
            except: return 0

        def fmt_vnd(val):
            try:
                if pd.isna(val) or val == "": return "-"
                val = float(val)
                if val < 0: return "({:,.0f})".format(abs(val)).replace(",", ".")
                return "{:,.0f}".format(val).replace(",", ".")
            except: return str(val)

        def convert_df_to_excel(df):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            return output.getvalue()
        
        def parse_text_message(text):
            extracted = {}
            match_can = re.search(r'\b(phòng|căn|p|can)\s*[:.]?\s*(\d{3,4})', text, re.IGNORECASE)
            if match_can: extracted['ma_can'] = match_can.group(2)
            match_gia = re.search(r'(\d+)\s*(tr|triệu|k)', text, re.IGNORECASE)
            if match_gia:
                val = float(match_gia.group(1))
                extracted['gia_thue'] = val * 1000000 if ('tr' in match_gia.group(2) or 'triệu' in match_gia.group(2)) else val * 1000
            dates = re.findall(r'(\d{1,2}[/-]\d{1,2}[/-]?\d{0,4})', text)
            if len(dates) >= 1: extracted['ngay_in'] = dates[0]
            if len(dates) >= 2: extracted['ngay_out'] = dates[1]
            return extracted

        def parse_image_gemini(api_key, image):
            if not AI_AVAILABLE: return None
            try:
                client = genai.Client(api_key=api_key)
                prompt = """Trích xuất JSON: {"ma_can": "số phòng", "ten_khach": "tên", "gia_thue": số_nguyên, "ngay_in": "YYYY-MM-DD", "ngay_out": "YYYY-MM-DD"}"""
                try: response = client.models.generate_content(model="gemini-1.5-flash", contents=[prompt, image])
                except: response = client.models.generate_content(model="gemini-1.5-pro", contents=[prompt, image])
                return json.loads(response.text.replace("```json", "").replace("```", "").strip())
            except: return None

        # --- HÀM GỘP DỮ LIỆU THÔNG MINH (LOGIC MỚI - MAX GIÁ, SUM TIỀN) ---
        def gop_du_lieu_phong(df):
            """
            Gộp các dòng có cùng Tòa và Mã căn.
            - Ngày: Lấy MAX (Để lấy ngày xa nhất/hợp lệ nhất).
            - Giá HĐ, Giá Thuê: Lấy MAX (Để không bị cộng dồn sai khi 1 dòng có giá, dòng kia = 0).
            - Tiền thanh toán/Cọc: Lấy SUM (Để cộng dồn tiền đã đóng rải rác).
            """
            if df.empty: return df
            
            # Định nghĩa quy tắc gộp (Aggregation Rules)
            agg_rules = {
                'Ngày ký': 'max', 'Ngày hết HĐ': 'max',
                'Ngày in': 'max', 'Ngày out': 'max',
                
                # QUAN TRỌNG: Dùng MAX cho giá để tránh cộng đôi
                'Giá HĐ': 'max', 
                'Giá': 'max', # Giá thuê khách
                
                # Dùng SUM cho các khoản thanh toán thực tế
                'TT cho chủ nhà': 'sum', 'Cọc cho chủ nhà': 'sum',
                'KH thanh toán': 'sum', 'KH cọc': 'sum',
                'Công ty': 'sum', 'Cá Nhân': 'sum',
                'SALE THẢO': 'sum', 'SALE NGA': 'sum', 'SALE LINH': 'sum',
                
                'Tên khách thuê': 'first'
            }
            
            final_agg = {k: v for k, v in agg_rules.items() if k in df.columns}
            
            cols_group = ['Toà', 'Mã căn']
            if not all(col in df.columns for col in cols_group): return df

            # Thực hiện gộp
            df_grouped = df.groupby(cols_group, as_index=False).agg(final_agg)
            return df_grouped

        # --- LOAD DATA ---
        df_main = load_data("HOP_DONG")
        df_cp = load_data("CHI_PHI")

        # --- CLEAN DATA ---
        if df_cp.empty:
            df_cp = pd.DataFrame(columns=COLUMNS_CP)
            df_cp["Ngày"] = pd.Series(dtype='datetime64[ns]')
            df_cp["Tiền"] = pd.Series(dtype='float')
        else:
            if "Chỉ số đồng hồ" not in df_cp.columns: df_cp["Chỉ số đồng hồ"] = ""
            if "Ngày" in df_cp.columns: df_cp["Ngày"] = pd.to_datetime(df_cp["Ngày"], errors='coerce')
            if "Tiền" in df_cp.columns: df_cp["Tiền"] = pd.to_numeric(df_cp["Tiền"], errors='coerce').fillna(0)
            df_cp["Mã căn"] = df_cp["Mã căn"].astype(str)
            df_cp["Loại"] = df_cp["Loại"].astype(str)
            df_cp["Chỉ số đồng hồ"] = df_cp["Chỉ số đồng hồ"].astype(str)

        if not df_main.empty:
            if "Mã căn" in df_main.columns: df_main["Mã căn"] = df_main["Mã căn"].astype(str)
            for c in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                if c in df_main.columns: df_main[c] = pd.to_datetime(df_main[c], errors='coerce')
            
            cols_to_numeric = ["Giá", "Giá HĐ", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân", "TT cho chủ nhà", "Cọc cho chủ nhà", "KH thanh toán", "KH cọc"]
            for c in cols_to_numeric:
                if c in df_main.columns: df_main[c] = df_main[c].apply(to_num)

        # --- SIDEBAR NOTIFICATION ---
        with st.sidebar:
            st.divider()
            st.header("🔔 Trung Tâm Thông Báo")
            today = pd.Timestamp(date.today())
            if not df_main.empty:
                df_active = df_main.sort_values('Ngày out').groupby(['Mã căn', 'Toà']).tail(1).copy()
                df_hd = df_active[(df_active['Ngày hết HĐ'].notna()) & ((df_active['Ngày hết HĐ'] - today).dt.days.between(-999, 30))]
                df_kh = df_active[(df_active['Ngày out'].notna()) & ((df_active['Ngày out'] - today).dt.days.between(0, 7))]

                if df_hd.empty and df_kh.empty: st.success("✅ Hệ thống ổn định.")
                else:
                    if not df_hd.empty:
                        st.error(f"🔴 {len(df_hd)} Hợp đồng cần xử lý")
                        for _, r in df_hd.iterrows():
                             d = (r['Ngày hết HĐ']-today).days
                             msg = "Đã hết hạn" if d < 0 else f"Còn {d} ngày"
                             toa_info = f" ({r['Toà']})" if str(r['Toà']).strip() != '' else ''
                             st.caption(f"🏠 {r['Mã căn']}{toa_info}: {msg}")
                    if not df_kh.empty:
                        st.warning(f"🟡 {len(df_kh)} Khách sắp trả phòng")
                        for _, r in df_kh.iterrows(): 
                            toa_info = f" ({r['Toà']})" if str(r['Toà']).strip() != '' else ''
                            st.caption(f"🚪 {r['Mã căn']}{toa_info}: {(r['Ngày out']-today).days} ngày")
            
            st.divider()
            if st.button("🔄 Tải lại dữ liệu (F5)", use_container_width=True): 
                st.cache_data.clear()
                st.rerun()

        DANH_SACH_NHA = { "Tòa A": ["A101"], "Tòa B": ["B101"], "Khác": [] } # Demo Config

        # --- MAIN TABS ---
        tabs = st.tabs([
            "✍️ Nhập Liệu", "📥 Upload Excel", "💸 Chi Phí Nội Bộ", 
            "📋 Dữ Liệu Gốc", "🏠 Cảnh Báo", 
            "💰 Quản Lý Chi Phí", "📊 P&L (Lợi Nhuận)", "💸 Dòng Tiền" 
        ])

        # ... (TAB 1, 2, 3, 4, 5 GIỮ NGUYÊN NHƯ CŨ - ĐÃ LƯỢC BỚT ĐỂ TẬP TRUNG VÀO TAB SAU) ...
        # (Để code chạy được trơn tru, tôi sẽ giữ phần khung Tab 1-5 cơ bản nhất)
        
        with tabs[0]:
            st.subheader("✍️ Nhập Liệu Hợp Đồng Mới")
            with st.form("main_form"):
                c1, c2, c3, c4 = st.columns(4)
                with c1: toa = st.text_input("Tòa nhà", "MT60")
                with c2: can = st.text_input("Mã căn")
                with c3: price = st.number_input("Giá thuê")
                with c4: submitted = st.form_submit_button("Lưu Demo")
                if submitted: st.success("Đã lưu (Demo)")

        with tabs[1]: st.info("Chức năng Upload Excel (Giữ nguyên code cũ)")
        with tabs[2]: st.info("Chức năng Chi Phí Nội Bộ (Giữ nguyên code cũ)")
        with tabs[3]: st.info("Chức năng Dữ Liệu Gốc (Giữ nguyên code cũ)")
        with tabs[4]: st.info("Chức năng Cảnh Báo (Giữ nguyên code cũ)")

        # ---------------------------------------------------------
        # TAB 6: QUẢN LÝ CHI PHÍ (DETAIL VIEW - ĐÃ GỘP DÒNG)
        # ---------------------------------------------------------
        with tabs[5]:
            st.subheader("💰 Quản Lý Chi Phí & Doanh Thu (Đã Gộp Dữ Liệu)")
            if not df_main.empty:
                # --- ÁP DỤNG LOGIC GỘP MỚI ---
                df_agg = gop_du_lieu_phong(df_main)
                
                cols_to_show = ["Toà", "Mã căn", "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Giá", "KH thanh toán", "KH cọc", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân"]
                cols_with_dates = cols_to_show + ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]
                existing_cols = [c for c in cols_with_dates if c in df_agg.columns]
                
                df_view = df_agg[existing_cols].copy()
                df_view = df_view.rename(columns={"TT cho chủ nhà": "Thanh toán HĐ", "Cọc cho chủ nhà": "Cọc HĐ", "Giá": "Giá thuê", "KH thanh toán": "Khách thanh toán", "KH cọc": "Khách cọc", "Công ty": "HH Công ty", "Cá Nhân": "HH Cá nhân"})
                if "Mã căn" in df_view.columns: df_view = df_view.sort_values(by=["Toà", "Mã căn"])
                
                def make_note(row):
                    def d(x): return x.strftime('%d/%m/%y') if not pd.isna(x) else "?"
                    k = d(row.get('Ngày ký')); h = d(row.get('Ngày hết HĐ')); i = d(row.get('Ngày in')); o = d(row.get('Ngày out'))
                    return f"HĐ: {k}-{h} | Khách: {i}-{o}"
                
                df_view["Ghi chú"] = df_view.apply(make_note, axis=1)
                df_view = df_view.drop(columns=["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"], errors='ignore')
                
                numeric_cols = ["Giá HĐ", "Thanh toán HĐ", "Cọc HĐ", "Giá thuê", "Khách thanh toán", "Khách cọc", "SALE THẢO", "SALE NGA", "SALE LINH", "HH Công ty", "HH Cá nhân"]
                total_row = pd.DataFrame(df_view[numeric_cols].sum(numeric_only=True)).T; total_row["Toà"] = "TỔNG CỘNG"; total_row = total_row.fillna("")
                df_final_view = pd.concat([df_view, total_row], ignore_index=True)
                for col in numeric_cols: 
                    if col in df_final_view.columns: df_final_view[col] = df_final_view[col].apply(fmt_vnd)
                
                st.dataframe(
                    df_final_view.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}),
                    use_container_width=True, 
                    column_config={"Ghi chú": st.column_config.TextColumn(width="medium", help="Thông tin ngày tháng hợp đồng")}
                )
            else: st.info("Chưa có dữ liệu.")

        # ---------------------------------------------------------
        # TAB 7: TỔNG HỢP CHI PHÍ (P&L) - ĐÃ GỘP DÒNG
        # ---------------------------------------------------------
        with tabs[6]:
            st.subheader("📊 Báo Cáo Lợi Nhuận (Profit & Loss)")
            
            c_filter1, c_filter2 = st.columns(2)
            with c_filter1: sel_month = st.selectbox("Chọn Tháng", range(1, 13), index=date.today().month - 1, key="pl_month")
            with c_filter2: sel_year = st.number_input("Chọn Năm", min_value=2020, max_value=2030, value=date.today().year, key="pl_year")
            st.divider()

            if not df_main.empty:
                # 1. GỘP DỮ LIỆU TRƯỚC (QUAN TRỌNG)
                df_merged = gop_du_lieu_phong(df_main)
                
                # 2. SAU ĐÓ MỚI TÍNH TOÁN
                df_calc = df_merged.copy()
                
                def get_months(start, end):
                    if pd.isna(start) or pd.isna(end): return 0
                    try: return max(0, (end - start).days / 30)
                    except: return 0
                
                df_calc['Tháng HĐ'] = df_calc.apply(lambda r: get_months(r['Ngày ký'], r['Ngày hết HĐ']), axis=1)
                df_calc['Tổng giá trị HĐ'] = (df_calc['Giá HĐ'] * df_calc['Tháng HĐ'])
                
                df_calc['Tháng Thuê'] = df_calc.apply(lambda r: get_months(r['Ngày in'], r['Ngày out']), axis=1)
                df_calc['Chi phí vốn (theo khách)'] = (df_calc['Giá HĐ'] * df_calc['Tháng Thuê'])
                df_calc['Doanh thu cho thuê'] = (df_calc['Giá'] * df_calc['Tháng Thuê'])
                
                df_calc['Tổng Chi Phí Sale'] = df_calc['SALE THẢO'] + df_calc['SALE NGA'] + df_calc['SALE LINH']
                df_calc['Lợi nhuận ròng'] = df_calc['Doanh thu cho thuê'] - df_calc['Chi phí vốn (theo khách)'] - df_calc['Tổng Chi Phí Sale'] - df_calc['Công ty'] - df_calc['Cá Nhân']

                # Metrics
                total_rev = df_calc['Doanh thu cho thuê'].sum()
                total_cost = df_calc['Chi phí vốn (theo khách)'].sum() + df_calc['Tổng Chi Phí Sale'].sum() + df_calc['Công ty'].sum() + df_calc['Cá Nhân'].sum()
                total_net = df_calc['Lợi nhuận ròng'].sum()
                
                c_m1, c_m2, c_m3 = st.columns(3)
                c_m1.metric("💰 TỔNG DOANH THU", fmt_vnd(total_rev))
                c_m2.metric("📉 TỔNG CHI PHÍ & VỐN", fmt_vnd(total_cost))
                c_m3.metric("💎 TỔNG LỢI NHUẬN", fmt_vnd(total_net), delta=fmt_vnd(total_net), delta_color="normal" if total_net > 0 else "inverse")
                
                st.divider()

                # Bảng chi tiết
                def make_smart_note(row, profit, cogs):
                    def d(x): return x.strftime('%d/%m/%y') if not pd.isna(x) else "?"
                    k = d(row.get('Ngày ký')); h = d(row.get('Ngày hết HĐ')); i = d(row.get('Ngày in')); o = d(row.get('Ngày out'))
                    base = f"HĐ: {k}-{h} | Khách: {i}-{o}"
                    warn = []
                    if cogs == 0 and profit == 0: warn.append("⚠️ Thiếu ngày")
                    elif profit < 0: warn.append("📉 Lỗ")
                    if warn: base += " || " + " ".join(warn)
                    return base

                df_calc["Ghi chú"] = df_calc.apply(lambda r: make_smart_note(r, r['Lợi nhuận ròng'], r['Chi phí vốn (theo khách)']), axis=1)
                
                cols_final = ["Toà", "Mã căn", "Tổng giá trị HĐ", "Chi phí vốn (theo khách)", "Doanh thu cho thuê", "Tổng Chi Phí Sale", "Công ty", "Cá Nhân", "Lợi nhuận ròng", "Ghi chú"]
                if "Mã căn" in df_calc.columns: df_calc = df_calc.sort_values(by=["Toà", "Mã căn"])
                
                df_show = df_calc[cols_final].copy()
                total_row = pd.DataFrame(df_show.sum(numeric_only=True)).T; total_row["Toà"] = "TỔNG CỘNG"; total_row = total_row.fillna("")
                df_res = pd.concat([df_show, total_row], ignore_index=True)
                
                def highlight(val): 
                    if isinstance(val, (int, float)): return 'color: red; font-weight: bold' if val < 0 else 'color: green; font-weight: bold' if val > 0 else ''
                    return ''
                
                num_cols = ["Tổng giá trị HĐ", "Chi phí vốn (theo khách)", "Doanh thu cho thuê", "Tổng Chi Phí Sale", "Công ty", "Cá Nhân", "Lợi nhuận ròng"]
                st.dataframe(
                    df_res.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}).applymap(highlight, subset=["Lợi nhuận ròng"]).format("{:,.0f}", subset=pd.IndexSlice[0:len(df_res)-1, num_cols]),
                    use_container_width=True, 
                    column_config={"Ghi chú": st.column_config.TextColumn(width="large")}
                )
            else: st.info("Chưa có dữ liệu.")

        # ---------------------------------------------------------
        # TAB 8: QUẢN LÝ DÒNG TIỀN (CASHFLOW) - ĐÃ GỘP DÒNG
        # ---------------------------------------------------------
        with tabs[7]:
            st.subheader("💸 Quản Lý Dòng Tiền (Thực Thu - Thực Chi)")
            
            if not df_main.empty:
                # 1. GỘP DỮ LIỆU
                df_cf = gop_du_lieu_phong(df_main)
                
                # 2. TÍNH TOÁN
                df_cf['Thu: Thanh toán'] = df_cf['KH thanh toán']
                df_cf['Thu: Cọc'] = df_cf['KH cọc']
                df_cf['TỔNG THU'] = df_cf['Thu: Thanh toán'] + df_cf['Thu: Cọc']
                
                df_cf['Chi: Chủ nhà'] = df_cf['TT cho chủ nhà'] + df_cf['Cọc cho chủ nhà']
                df_cf['Chi: Hoa hồng'] = df_cf['SALE THẢO'] + df_cf['SALE NGA'] + df_cf['SALE LINH'] + df_cf['Công ty'] + df_cf['Cá Nhân']
                
                # Chi phí vận hành
                df_op_cost = pd.DataFrame()
                if not df_cp.empty:
                    df_op_cost = df_cp.groupby("Mã căn")["Tiền"].sum().reset_index()
                    df_op_cost.columns = ["Mã căn", "Chi: Vận hành"]
                
                df_final_cf = pd.merge(df_cf, df_op_cost, on="Mã căn", how="left").fillna(0)
                df_final_cf['TỔNG CHI'] = df_final_cf['Chi: Chủ nhà'] + df_final_cf['Chi: Hoa hồng'] + df_final_cf['Chi: Vận hành']
                df_final_cf['DÒNG TIỀN RÒNG'] = df_final_cf['TỔNG THU'] - df_final_cf['TỔNG CHI']
                
                # Metrics
                c_cf1, c_cf2, c_cf3 = st.columns(3)
                tot_in = df_final_cf['TỔNG THU'].sum()
                tot_out = df_final_cf['TỔNG CHI'].sum()
                net_cf = tot_in - tot_out
                
                c_cf1.metric("💰 TỔNG THỰC THU", fmt_vnd(tot_in))
                c_cf2.metric("💸 TỔNG THỰC CHI", fmt_vnd(tot_out))
                c_cf3.metric("💎 DÒNG TIỀN RÒNG", fmt_vnd(net_cf), delta_color="normal" if net_cf > 0 else "inverse")
                
                st.divider()

                # Ghi chú
                def explain_cf(row):
                    net = row['DÒNG TIỀN RÒNG']
                    if net >= 0: return "✅ Dương"
                    reasons = []
                    if row['TỔNG THU'] == 0: reasons.append("⚠️ Chưa thu")
                    elif row['Chi: Chủ nhà'] > 0 and row['TỔNG THU'] < row['Chi: Chủ nhà']: reasons.append("⚠️ Chi > Thu")
                    return "; ".join(reasons)

                df_final_cf['Ghi chú'] = df_final_cf.apply(explain_cf, axis=1)

                cols_cf_show = ["Toà", "Mã căn", "Thu: Thanh toán", "Thu: Cọc", "TỔNG THU", "Chi: Chủ nhà", "Chi: Hoa hồng", "Chi: Vận hành", "TỔNG CHI", "DÒNG TIỀN RÒNG", "Ghi chú"]
                if "Mã căn" in df_final_cf.columns: df_final_cf = df_final_cf.sort_values(by=["Toà", "Mã căn"])
                df_cf_display = df_final_cf[cols_cf_show].copy()
                
                total_row_cf = pd.DataFrame(df_cf_display.sum(numeric_only=True)).T; total_row_cf["Toà"] = "TỔNG CỘNG"; total_row_cf = total_row_cf.fillna("")
                df_cf_result = pd.concat([df_cf_display, total_row_cf], ignore_index=True)
                
                def highlight_cf(val): 
                    if isinstance(val, (int, float)): return 'color: red; font-weight: bold' if val < 0 else 'color: green; font-weight: bold' if val > 0 else ''
                    return ''
                
                num_cols_cf = ["Thu: Thanh toán", "Thu: Cọc", "TỔNG THU", "Chi: Chủ nhà", "Chi: Hoa hồng", "Chi: Vận hành", "TỔNG CHI", "DÒNG TIỀN RÒNG"]
                st.dataframe(
                    df_cf_result.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}).applymap(highlight_cf, subset=["DÒNG TIỀN RÒNG"]).format("{:,.0f}", subset=pd.IndexSlice[0:len(df_cf_result)-1, num_cols_cf]),
                    use_container_width=True, 
                    column_config={"Ghi chú": st.column_config.TextColumn(width="large")}
                )
            else: st.info("Chưa có dữ liệu.")

else:
    st.warning("👈 Vui lòng tải file **JSON Chìa Khóa** từ Google lên đây.")