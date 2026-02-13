import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
import os
import json
import re
import time
import io
# from PIL import Image # Giữ dòng này nếu bạn cần dùng tính năng đọc ảnh

# --- THƯ VIỆN KẾT NỐI GOOGLE SHEETS ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================

st.set_page_config(
    page_title="MT60 Cloud Manager", 
    layout="wide", 
    page_icon="☁️",
    initial_sidebar_state="expanded"
)

# --- CSS: TÙY CHỈNH GIAO DIỆN COMPACT ---
st.markdown("""
    <style>
        .block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; }
        div[data-testid="stVerticalBlock"] { gap: 0.2rem !important; }
        div[data-testid="stDataFrame"] { width: 100%; }
        /* Tùy chỉnh thanh cuộn */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-thumb { background: #888; border-radius: 3px; }
    </style>
""", unsafe_allow_html=True)

# Kiểm tra thư viện AI
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

# ==============================================================================
# 2. KẾT NỐI DỮ LIỆU
# ==============================================================================

st.title("☁️ MT60 STUDIO - QUẢN LÝ TỔNG QUAN")
st.markdown("---")

st.sidebar.header("🔐 Đăng Nhập")
uploaded_key = st.sidebar.file_uploader("Chọn file JSON để mở khóa", type=['json'])

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

# ==============================================================================
# 3. XỬ LÝ LOGIC CHÍNH
# ==============================================================================

if uploaded_key is not None:
    uploaded_key.seek(0)
    with st.spinner("Đang tải dữ liệu..."):
        sh = connect_google_sheet(uploaded_key)
    
    if sh:
        st.sidebar.success("✅ Đã kết nối!")
        
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

        # --- HÀM CHUYỂN ĐỔI SỐ AN TOÀN ---
        def to_num(val):
            if isinstance(val, (int, float)): return float(val)
            if isinstance(val, str): 
                # Xóa dấu chấm và phẩy để tránh nhầm lẫn
                clean_val = val.replace(',', '').replace('.', '').strip()
                if clean_val == '' or clean_val.lower() == 'nan': return 0
                try: return float(clean_val)
                except: return 0
            return 0

        # --- HÀM FORMAT ĐỂ HIỂN THỊ (QUAN TRỌNG: TRẢ VỀ STRING) ---
        def fmt_vnd(val):
            try:
                val = float(val) # Đảm bảo là số trước khi format
                if pd.isna(val): return "-"
                # Format: 1.000.000 (Dấu chấm phân cách ngàn, không số lẻ)
                if val < 0: return "({:,.0f})".format(abs(val)).replace(",", ".")
                return "{:,.0f}".format(val).replace(",", ".")
            except: return str(val)

        def fmt_date(val):
            try:
                if pd.isna(val) or val == "": return ""
                if isinstance(val, str):
                    val = pd.to_datetime(val, errors='coerce')
                if pd.isna(val): return ""
                return val.strftime('%d/%m/%y') # Format dd/mm/yy
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
        
        def parse_text_message(text):
            extracted = {}
            match_can = re.search(r'\b(phòng|căn|p|can)\s*[:.]?\s*(\d{3,4})', text, re.IGNORECASE)
            if match_can: extracted['ma_can'] = match_can.group(2)
            match_gia = re.search(r'(\d+)\s*(tr|triệu|k)', text, re.IGNORECASE)
            if match_gia:
                val = float(match_gia.group(1))
                if 'tr' in match_gia.group(2) or 'triệu' in match_gia.group(2):
                    extracted['gia_thue'] = val * 1000000 
                else:
                    extracted['gia_thue'] = val * 1000
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

        # --- HÀM GỘP DỮ LIỆU ---
        def gop_du_lieu_phong(df_input):
            if df_input.empty: return df_input
            df = df_input.copy()
            
            # Chuẩn hóa tên cột (xóa khoảng trắng thừa nếu có)
            df.columns = df.columns.str.strip()

            # --- CHỐT CHẶN: Ép toàn bộ cột tiền về dạng số thực (float) ---
            numeric_cols_force = [
                "Giá HĐ", "Giá", 
                "TT cho chủ nhà", "Cọc cho chủ nhà", 
                "KH thanh toán", "KH cọc", 
                "Công ty", "Cá Nhân", 
                "SALE THẢO", "SALE NGA", "SALE LINH"
            ]
            for col in numeric_cols_force:
                if col in df.columns:
                    # Xóa ký tự lạ, chuyển về số, lỗi -> 0.0
                    if df[col].dtype == object:
                        df[col] = df[col].astype(str).str.replace('.', '').str.replace(',', '')
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

            # Tạo ghi chú
            def tao_mo_ta_dong(row):
                details = []
                def d(x): return x.strftime('%d/%m/%y') if not pd.isna(x) else "?"
                k, h = d(row.get('Ngày ký')), d(row.get('Ngày hết HĐ'))
                i, o = d(row.get('Ngày in')), d(row.get('Ngày out'))
                
                if k != "?" or h != "?": details.append(f"HĐ({k}-{h})")
                if row.get('Giá HĐ', 0) > 0: details.append(f"GiáHĐ:{fmt_vnd(row['Giá HĐ'])}")
                if i != "?" or o != "?": details.append(f"Khách({i}-{o})")
                if row.get('Giá', 0) > 0: details.append(f"GiáThuê:{fmt_vnd(row['Giá'])}")
                
                thu = row.get('KH thanh toán', 0) + row.get('KH cọc', 0)
                if thu > 0: details.append(f"Thu:{fmt_vnd(thu)}")
                chi = row.get('TT cho chủ nhà', 0) + row.get('Cọc cho chủ nhà', 0)
                if chi > 0: details.append(f"Chi:{fmt_vnd(chi)}")
                
                if not details: return "Trống"
                return ", ".join(details)

            df['_chi_tiet_nhap'] = df.apply(tao_mo_ta_dong, axis=1)

            # Quy tắc gộp
            agg_rules = {
                'Ngày ký': 'min', 'Ngày hết HĐ': 'max',
                'Ngày in': 'min', 'Ngày out': 'max',
                'Giá HĐ': 'max', 'Giá': 'max', # Giá lấy Max
                'TT cho chủ nhà': 'sum', 'Cọc cho chủ nhà': 'sum',
                'KH thanh toán': 'sum', 'KH cọc': 'sum',
                'Công ty': 'sum', 'Cá Nhân': 'sum',
                'SALE THẢO': 'sum', 'SALE NGA': 'sum', 'SALE LINH': 'sum',
                'Tên khách thuê': 'first',
                '_chi_tiet_nhap': lambda x: '\n'.join([f"• Lần {i+1}: {v}" for i, v in enumerate(x) if v != "Trống"])
            }
            
            final_agg = {k: v for k, v in agg_rules.items() if k in df.columns}
            cols_group = ['Toà', 'Mã căn']
            
            if not all(col in df.columns for col in cols_group): return df

            df_grouped = df.groupby(cols_group, as_index=False).agg(final_agg)
            df_grouped = df_grouped.rename(columns={'_chi_tiet_nhap': 'Ghi chú'})
            return df_grouped

        # --- LOAD DATA ---
        df_main = load_data("HOP_DONG")
        df_cp = load_data("CHI_PHI")

        # Clean CP
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

        # Clean Hop Dong
        if not df_main.empty:
            df_main.columns = df_main.columns.str.strip() # Xóa khoảng trắng tên cột
            if "Mã căn" in df_main.columns: df_main["Mã căn"] = df_main["Mã căn"].astype(str)
            for c in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                if c in df_main.columns: df_main[c] = pd.to_datetime(df_main[c], errors='coerce')
            
            cols_to_numeric = ["Giá", "Giá HĐ", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân", "TT cho chủ nhà", "Cọc cho chủ nhà", "KH thanh toán", "KH cọc"]
            for c in cols_to_numeric:
                if c in df_main.columns: df_main[c] = df_main[c].apply(to_num)

        # --- SIDEBAR NOTIFICATION ---
        with st.sidebar:
            st.divider()
            st.header("🔔 Thông Báo")
            today = pd.Timestamp(date.today())
            if not df_main.empty:
                df_active = df_main.sort_values('Ngày out').groupby(['Mã căn', 'Toà']).tail(1).copy()
                df_hd = df_active[(df_active['Ngày hết HĐ'].notna()) & ((df_active['Ngày hết HĐ'] - today).dt.days.between(-999, 30))]
                df_kh = df_active[(df_active['Ngày out'].notna()) & ((df_active['Ngày out'] - today).dt.days.between(0, 7))]

                if df_hd.empty and df_kh.empty: st.success("✅ Ổn định")
                else:
                    if not df_hd.empty:
                        st.error(f"🔴 {len(df_hd)} Hợp đồng cần xử lý")
                        for _, r in df_hd.iterrows():
                             days_left = (r['Ngày hết HĐ'] - today).days
                             status_msg = "ĐÃ HẾT HẠN" if days_left < 0 else f"Còn {days_left} ngày"
                             toa_nha = str(r['Toà']).strip() if str(r['Toà']).strip() != '' else "Chưa rõ"
                             phong = str(r['Mã căn']).strip()
                             st.markdown(f"""
                             <div style="border-bottom: 1px solid rgba(49, 51, 63, 0.2); padding-bottom: 4px; margin-bottom: 4px;">
                                <strong style="color: #FF4B4B;">🏠 {toa_nha} - P.{phong}</strong><br>
                                <span style="font-size: 0.9em; color: #555;">⚠️ {status_msg} (Hết: {fmt_date(r['Ngày hết HĐ'])})</span>
                             </div>
                             """, unsafe_allow_html=True)
                    if not df_kh.empty:
                        st.warning(f"🟡 {len(df_kh)} Khách sắp out")
                        for _, r in df_kh.iterrows(): 
                            days_left = (r['Ngày out'] - today).days
                            toa_nha = str(r['Toà']).strip() if str(r['Toà']).strip() != '' else "Chưa rõ"
                            phong = str(r['Mã căn']).strip()
                            ten_khach = str(r['Tên khách thuê']).strip()
                            st.markdown(f"""
                             <div style="border-bottom: 1px solid rgba(49, 51, 63, 0.2); padding-bottom: 4px; margin-bottom: 4px;">
                                <strong style="color: #FFA500;">🚪 {toa_nha} - P.{phong}</strong><br>
                                <span style="font-size: 0.9em; color: #333;">👤 {ten_khach}</span><br>
                                <span style="font-size: 0.85em; color: #666;">⏳ Còn {days_left} ngày (Out: {fmt_date(r['Ngày out'])})</span>
                             </div>
                             """, unsafe_allow_html=True)
            st.divider()
            if st.button("🔄 Tải lại dữ liệu", use_container_width=True): 
                st.cache_data.clear()
                st.rerun()

        DANH_SACH_NHA = { "Tòa A": ["A101"], "Tòa B": ["B101"], "Khác": [] }

        # ==============================================================================
        # 4. GIAO DIỆN CHÍNH (TABS)
        # ==============================================================================
        tabs = st.tabs([
            "✍️ Nhập Liệu", "📥 Upload Excel", "💸 Chi Phí Nội Bộ", 
            "📋 Dữ Liệu Gốc", "🏠 Cảnh Báo", 
            "💰 Quản Lý Chi Phí", "📊 P&L (Lợi Nhuận)", "💸 Dòng Tiền",
            "📅 Quyết Toán Tháng" 
        ])

        # --- TAB 1 ---
        with tabs[0]:
            st.subheader("✍️ Nhập Liệu Hợp Đồng Mới")
            with st.expander("🛠️ Công cụ hỗ trợ", expanded=False):
                c_txt, c_img = st.columns(2)
                with c_txt:
                    txt = st.text_area("Dán tin nhắn Zalo:")
                    if st.button("Phân tích Text"): st.session_state['auto'] = parse_text_message(txt)
                with c_img:
                    key_vis = st.text_input("API Key Vision", type="password")
                    up = st.file_uploader("Upload ảnh HĐ", type=["jpg", "png"])
                    if up and key_vis and st.button("Phân tích Ảnh"):
                        # from PIL import Image # Đã import ở đầu
                        with st.spinner("AI đang đọc..."): st.session_state['auto'] = parse_image_gemini(key_vis, Image.open(up))
            st.divider()
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
                with c34: tt_chu_nha = st.text_input("TT cho chủ nhà")
                c41, c42, c43, c44 = st.columns(4)
                with c41: sale_thao = st.number_input("Sale Thảo", step=50000)
                with c42: sale_nga = st.number_input("Sale Nga", step=50000)
                with c43: sale_linh = st.number_input("Sale Linh", step=50000)
                with c44: cong_ty = st.number_input("Công ty", step=50000)
                
                if st.form_submit_button("💾 LƯU HỢP ĐỒNG MỚI", type="primary"):
                    new_data = {"Tòa nhà": chon_toa, "Mã căn": chon_can, "Toà": chon_toa, "Chủ nhà - sale": chu_nha_sale, 
                                "Ngày ký": pd.to_datetime(ngay_ky), "Ngày hết HĐ": pd.to_datetime(ngay_het_hd), "Giá HĐ": gia_hd,
                                "TT cho chủ nhà": tt_chu_nha, "Tên khách thuê": ten_khach, "Ngày in": pd.to_datetime(ngay_in), "Ngày out": pd.to_datetime(ngay_out),
                                "Giá": gia_thue, "KH cọc": kh_coc, "Công ty": cong_ty, "SALE THẢO": sale_thao, "SALE NGA": sale_nga, "SALE LINH": sale_linh,
                                "Cọc cho chủ nhà": "", "KH thanh toán": "", "Cá Nhân": "", "Hết hạn khách hàng": "", "Ráp khách khi hết hạn": ""}
                    df_final = pd.concat([df_main, pd.DataFrame([new_data])], ignore_index=True)
                    save_data(df_final, "HOP_DONG"); st.session_state['auto'] = {}; time.sleep(1); st.rerun()

        with tabs[1]:
            st.header("📤 Quản lý File Excel")
            st.download_button("📥 Tải File Mẫu", convert_df_to_excel(pd.DataFrame(columns=COLUMNS)), "mau_hop_dong.xlsx")
            up = st.file_uploader("Upload Excel", type=["xlsx"], key="up_main")
            if up and st.button("🚀 ĐỒNG BỘ CLOUD"):
                try:
                    df_up = pd.read_excel(up)
                    for col in COLUMNS: 
                        if col not in df_up.columns: df_up[col] = ""
                    df_up = df_up[COLUMNS]
                    for col in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                        if col in df_up.columns: df_up[col] = pd.to_datetime(df_up[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    save_data(df_up, "HOP_DONG"); time.sleep(2); st.rerun()
                except Exception as e: st.error(f"Lỗi: {e}")

        with tabs[2]:
            st.subheader("💸 Chi Phí Nội Bộ")
            with st.expander("🧮 Nhập nhanh", expanded=True):
                with st.form("cp_form"):
                    c1, c2, c3, c4 = st.columns(4)
                    d = c1.date_input("Ngày", date.today()); can = c2.text_input("Mã căn")
                    loai = c3.selectbox("Loại", ["Điện", "Nước", "Net", "Dọn dẹp", "Khác"])
                    tien = c4.number_input("Tiền", step=10000.0)
                    if st.form_submit_button("Lưu"):
                        new = pd.DataFrame([{"Mã căn": can, "Loại": loai, "Tiền": tien, "Ngày": pd.to_datetime(d), "Chỉ số đồng hồ": ""}])
                        save_data(pd.concat([df_cp, new], ignore_index=True), "CHI_PHI"); time.sleep(1); st.rerun()
            st.divider(); st.subheader("Upload Excel Chi Phí")
            up_cp = st.file_uploader("File Chi Phí", type=["xlsx"], key="up_cp")
            if up_cp and st.button("🚀 ĐỒNG BỘ CHI PHÍ"):
                try:
                    df_up_cp = pd.read_excel(up_cp)
                    if "Ngày" in df_up_cp.columns: df_up_cp["Ngày"] = pd.to_datetime(df_up_cp["Ngày"], errors='coerce')
                    if "Chỉ số đồng hồ" not in df_up_cp.columns: df_up_cp["Chỉ số đồng hồ"] = ""
                    df_comb = pd.concat([df_cp, df_up_cp[COLUMNS_CP]], ignore_index=True).drop_duplicates()
                    save_data(df_comb, "CHI_PHI"); time.sleep(1); st.rerun()
                except Exception as e: st.error(f"Lỗi: {e}")
            
            # Display CP with formatting (convert to string to avoid bugs)
            df_cp_show = df_cp.copy()
            df_cp_show["Tiền"] = df_cp_show["Tiền"].apply(fmt_vnd)
            st.dataframe(df_cp_show, use_container_width=True, column_config={"Ngày": st.column_config.DateColumn(format="DD/MM/YY")})

        with tabs[3]:
            st.subheader("📋 Dữ Liệu Gốc")
            # Format display for Raw Data
            df_main_show = df_main.copy()
            # Convert all numeric cols to formatted strings
            cols_money = ["Giá", "Giá HĐ", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân", "TT cho chủ nhà", "Cọc cho chủ nhà", "KH thanh toán", "KH cọc"]
            for c in cols_money:
                if c in df_main_show.columns: df_main_show[c] = df_main_show[c].apply(fmt_vnd)
                
            st.dataframe(
                df_main_show, 
                use_container_width=True,
                column_config={
                    "Ngày ký": st.column_config.DateColumn(format="DD/MM/YY"),
                    "Ngày hết HĐ": st.column_config.DateColumn(format="DD/MM/YY"),
                    "Ngày in": st.column_config.DateColumn(format="DD/MM/YY"), 
                    "Ngày out": st.column_config.DateColumn(format="DD/MM/YY"),
                }
            )

        with tabs[4]:
            st.info("Xem thông báo chi tiết ở thanh bên trái (Sidebar).")

        # --- TAB 6: QUẢN LÝ CHI PHÍ (GỘP) ---
        with tabs[5]:
            st.subheader("💰 Quản Lý Chi Phí & Doanh Thu")
            if not df_main.empty:
                df_agg = gop_du_lieu_phong(df_main)
                cols_show = ["Toà", "Mã căn", "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Giá", "KH thanh toán", "KH cọc", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân", "Ghi chú"]
                cols_exist = [c for c in cols_show if c in df_agg.columns]
                df_view = df_agg[cols_exist].copy()
                
                num_cols = ["Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Giá", "KH thanh toán", "KH cọc", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân"]
                # Save numeric for export
                df_export_6 = df_view.copy() 
                # Convert to string for display to avoid 2^53 limits
                for c in num_cols: 
                    if c in df_view.columns: df_view[c] = df_view[c].apply(fmt_vnd)
                
                st.dataframe(
                    df_view.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}), 
                    use_container_width=True, 
                    column_config={"Ghi chú": st.column_config.TextColumn(width=500)}
                )
                
                st.download_button("📥 Tải Bảng Excel", convert_df_to_excel(df_export_6), "QuanLyChiPhi.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                st.divider(); st.write("##### 🔎 Soi Chi Tiết")
                sel_phong = st.selectbox("Chọn Phòng:", df_view['Mã căn'].unique(), key="sel_t6")
                if sel_phong: st.text_area("Nội dung:", df_view[df_view['Mã căn']==sel_phong]['Ghi chú'].values[0], height=100)

        # --- TAB 7: P&L ---
        with tabs[6]:
            st.subheader("📊 Lợi Nhuận (All-time)")
            if not df_main.empty:
                df_merged = gop_du_lieu_phong(df_main)
                df_calc = df_merged.copy()
                
                def get_m(s, e): return max(0, (e-s).days/30) if pd.notna(s) and pd.notna(e) else 0
                
                df_calc['Doanh thu'] = df_calc.apply(lambda r: r['Giá'] * get_m(r['Ngày in'], r['Ngày out']), axis=1)
                df_calc['Giá vốn'] = df_calc.apply(lambda r: r['Giá HĐ'] * get_m(r['Ngày in'], r['Ngày out']), axis=1)
                df_calc['Chi phí Sale'] = df_calc['SALE THẢO'] + df_calc['SALE NGA'] + df_calc['SALE LINH'] + df_calc['Công ty'] + df_calc['Cá Nhân']
                df_calc['Lợi nhuận'] = df_calc['Doanh thu'] - df_calc['Giá vốn'] - df_calc['Chi phí Sale']
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Tổng Doanh Thu", fmt_vnd(df_calc['Doanh thu'].sum()))
                c2.metric("Tổng Giá Vốn + Sale", fmt_vnd(df_calc['Giá vốn'].sum() + df_calc['Chi phí Sale'].sum()))
                c3.metric("Tổng Lợi Nhuận", fmt_vnd(df_calc['Lợi nhuận'].sum()))
                
                df_show = df_calc[["Toà", "Mã căn", "Doanh thu", "Giá vốn", "Chi phí Sale", "Lợi nhuận", "Ghi chú"]]
                
                # Format to string to handle large numbers
                for c in ["Doanh thu", "Giá vốn", "Chi phí Sale", "Lợi nhuận"]:
                    df_show[c] = df_show[c].apply(fmt_vnd)

                st.dataframe(
                    df_show.style.applymap(lambda x: 'color: red' if "(" in str(x) else '', subset=['Lợi nhuận']), 
                    use_container_width=True, 
                    column_config={"Ghi chú": st.column_config.TextColumn(width=500)}
                )
                
                st.download_button("📥 Tải Báo Cáo P&L", convert_df_to_excel(df_calc), "BaoCaoLoiNhuan.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # --- TAB 8: DÒNG TIỀN ---
        with tabs[7]:
            st.subheader("💸 Dòng Tiền (Thực tế)")
            if not df_main.empty:
                df_cf = gop_du_lieu_phong(df_main)
                df_cf['Thu'] = df_cf['KH thanh toán'] + df_cf['KH cọc']
                df_cf['Chi'] = df_cf['TT cho chủ nhà'] + df_cf['Cọc cho chủ nhà'] + df_cf['SALE THẢO'] + df_cf['SALE NGA'] + df_cf['SALE LINH'] + df_cf['Công ty'] + df_cf['Cá Nhân']
                
                if not df_cp.empty:
                    cp_agg = df_cp.groupby('Mã căn')['Tiền'].sum().reset_index().rename(columns={'Tiền': 'Chi phí VH'})
                    df_cf = pd.merge(df_cf, cp_agg, on='Mã căn', how='left').fillna(0)
                    df_cf['Chi'] += df_cf['Chi phí VH']
                else: df_cf['Chi phí VH'] = 0
                
                df_cf['Ròng'] = df_cf['Thu'] - df_cf['Chi']
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Tổng Thu", fmt_vnd(df_cf['Thu'].sum()))
                c2.metric("Tổng Chi", fmt_vnd(df_cf['Chi'].sum()))
                c3.metric("Dòng Tiền Ròng", fmt_vnd(df_cf['Ròng'].sum()))
                
                df_cf_show = df_cf[["Toà", "Mã căn", "Thu", "Chi", "Chi phí VH", "Ròng", "Ghi chú"]].copy()
                # Format string for display
                for c in ["Thu", "Chi", "Chi phí VH", "Ròng"]:
                    df_cf_show[c] = df_cf_show[c].apply(fmt_vnd)

                st.dataframe(
                    df_cf_show.style.applymap(lambda x: 'color: red' if "(" in str(x) else '', subset=['Ròng']), 
                    use_container_width=True, 
                    column_config={"Ghi chú": st.column_config.TextColumn(width=500)}
                )
                st.download_button("📥 Tải Báo Cáo Dòng Tiền", convert_df_to_excel(df_cf), "BaoCaoDongTien.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # --- TAB 9: QUYẾT TOÁN THÁNG & THUẾ ---
        with tabs[8]:
            st.subheader("📅 Báo Cáo Tài Chính Hàng Tháng & Thuế")
            col_t1, col_t2, col_t3 = st.columns(3)
            with col_t1: q_month = st.selectbox("Tháng", range(1, 13), index=date.today().month - 1)
            with col_t2: q_year = st.number_input("Năm", value=date.today().year)
            with col_t3: tax_rate = st.number_input("Thuế khoán (%)", value=10.0, step=0.1) / 100.0
            
            st.divider()
            
            if not df_main.empty:
                df_month_base = gop_du_lieu_phong(df_main)
                start_date_mo = datetime(q_year, q_month, 1)
                if q_month == 12: end_date_mo = datetime(q_year + 1, 1, 1) - timedelta(days=1)
                else: end_date_mo = datetime(q_year, q_month + 1, 1) - timedelta(days=1)
                
                results_month = []
                for idx, row in df_month_base.iterrows():
                    cost_month = 0
                    if pd.notna(row['Ngày ký']) and pd.notna(row['Ngày hết HĐ']):
                        if row['Ngày ký'] <= end_date_mo and row['Ngày hết HĐ'] >= start_date_mo:
                            cost_month = row['Giá HĐ']
                    
                    rev_month = 0
                    if pd.notna(row['Ngày in']) and pd.notna(row['Ngày out']):
                        if row['Ngày in'] <= end_date_mo and row['Ngày out'] >= start_date_mo:
                            rev_month = row['Giá']
                    
                    if rev_month > 0 or cost_month > 0:
                        tax_amt = rev_month * tax_rate
                        net_profit = rev_month - cost_month - tax_amt
                        results_month.append({"Toà": row['Toà'], "Mã căn": row['Mã căn'], "Doanh thu tháng": rev_month, "Chi phí thuê (Vốn)": cost_month, "Thuế phải đóng": tax_amt, "Lợi nhuận ròng": net_profit, "Ghi chú": row['Ghi chú']})
                
                if results_month:
                    df_month_rep = pd.DataFrame(results_month)
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Tổng Doanh Thu", fmt_vnd(df_month_rep['Doanh thu tháng'].sum()))
                    m2.metric("Tổng Chi Phí Thuê", fmt_vnd(df_month_rep['Chi phí thuê (Vốn)'].sum()))
                    m3.metric("Tổng Thuế", fmt_vnd(df_month_rep['Thuế phải đóng'].sum()))
                    m4.metric("Lợi Nhuận Ròng", fmt_vnd(df_month_rep['Lợi nhuận ròng'].sum()), delta_color="normal" if df_month_rep['Lợi nhuận ròng'].sum() > 0 else "inverse")
                    
                    st.divider()
                    df_display = df_month_rep.copy()
                    # Convert to string for display
                    for c in ["Doanh thu tháng", "Chi phí thuê (Vốn)", "Thuế phải đóng", "Lợi nhuận ròng"]: 
                        df_display[c] = df_display[c].apply(fmt_vnd)
                    
                    st.dataframe(
                        df_display.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}), 
                        use_container_width=True, 
                        column_config={"Ghi chú": st.column_config.TextColumn(width=300)}
                    )
                    st.download_button("📥 Tải Báo Cáo Tháng", convert_df_to_excel(df_month_rep), f"BaoCaoThang_{q_month}_{q_year}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                else: st.warning(f"Không có dữ liệu trong tháng {q_month}/{q_year}")
            else: st.info("Chưa có dữ liệu.")

else:
    st.warning("👈 Vui lòng tải file **JSON Chìa Khóa** từ Google lên đây để bắt đầu.")