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

        # --- HÀM LÀM SẠCH SỐ TIỀN (ĐÃ FIX LỖI NHÂN 10 LẦN) ---
        def clean_money(val):
            """Xử lý mọi loại định dạng về số float chuẩn"""
            if pd.isna(val) or val == "": return 0.0
            
            # 1. Nếu Google Sheet trả về chuẩn là số (Int/Float), GIỮ NGUYÊN
            if isinstance(val, (int, float)): return float(val)
            
            # 2. Nếu là chuỗi văn bản (Do người dùng gõ tay có dấu phẩy)
            s = str(val).strip()
            if s == "": return 0.0
            
            # Xóa dấu phân cách phần ngàn (cả chấm và phẩy tùy thói quen)
            s = s.replace('.', '').replace(',', '')
            
            # Xóa các ký tự chữ cái dư thừa, chỉ giữ số và dấu trừ
            s = re.sub(r'[^\d-]', '', s)
            
            try: return float(s)
            except: return 0.0

        # --- HÀM ĐỊNH DẠNG HIỂN THỊ TRÊN WEB ---
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
            
            # Tạo ghi chú chi tiết
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

            # Quy tắc gộp (Các cột số đã được xử lý clean_money từ trước rồi nên rất an toàn)
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
        # 4. TẢI VÀ CHUẨN HÓA DỮ LIỆU ĐẦU VÀO (FIX LỖI MERGE)
        # ==============================================================================
        df_main = load_data("HOP_DONG")
        df_cp = load_data("CHI_PHI")

        # --- Clean Chi Phí ---
        if df_cp.empty:
            df_cp = pd.DataFrame(columns=COLUMNS_CP)
        else:
            df_cp.columns = df_cp.columns.str.strip()
            # FIX LỖI MÀN HÌNH ĐỎ: Ép buộc Mã Căn thành dạng Chuỗi (Text) và bỏ đuôi .0 nếu có
            if "Mã căn" in df_cp.columns: 
                df_cp["Mã căn"] = df_cp["Mã căn"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            
            if "Ngày" in df_cp.columns: df_cp["Ngày"] = pd.to_datetime(df_cp["Ngày"], errors='coerce')
            if "Tiền" in df_cp.columns: df_cp["Tiền"] = df_cp["Tiền"].apply(clean_money)

        # --- Clean Hợp Đồng ---
        if not df_main.empty:
            df_main.columns = df_main.columns.str.strip()
            # FIX LỖI MÀN HÌNH ĐỎ: Ép buộc Mã Căn thành dạng Chuỗi (Text)
            if "Mã căn" in df_main.columns: 
                df_main["Mã căn"] = df_main["Mã căn"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            
            for c in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                if c in df_main.columns: df_main[c] = pd.to_datetime(df_main[c], errors='coerce')
            
            # Ép kiểu tiền tệ ngay khi vừa tải về
            for c in COLS_MONEY:
                if c in df_main.columns: df_main[c] = df_main[c].apply(clean_money)

        # ==============================================================================
        # 5. SIDEBAR: THÔNG BÁO CHI TIẾT
        # ==============================================================================
        with st.sidebar:
            st.divider()
            st.header("🔔 Thông Báo")
            today = pd.Timestamp(date.today())
            if not df_main.empty:
                df_alert_base = gop_du_lieu_phong(df_main)
                
                df_hd = df_alert_base[(df_alert_base['Ngày hết HĐ'].notna()) & ((df_alert_base['Ngày hết HĐ'] - today).dt.days.between(-999, 30))]
                df_kh = df_alert_base[(df_alert_base['Ngày out'].notna()) & ((df_alert_base['Ngày out'] - today).dt.days.between(0, 7))]

                if df_hd.empty and df_kh.empty: st.success("✅ Ổn định")
                else:
                    if not df_hd.empty:
                        st.error(f"🔴 {len(df_hd)} Hợp đồng cần xử lý")
                        for _, r in df_hd.iterrows():
                             days_left = (r['Ngày hết HĐ'] - today).days
                             status_msg = "ĐÃ HẾT HẠN" if days_left < 0 else f"Còn {days_left} ngày"
                             toa_nha = str(r.get('Toà', 'Chưa rõ')).strip()
                             with st.expander(f"🏠 {toa_nha} - P.{r['Mã căn']} ({status_msg})"):
                                 st.write(f"**Chủ/Sale:** {r.get('Chủ nhà - sale', 'N/A')}")
                                 st.write(f"**Giá HĐ:** {fmt_vnd(r.get('Giá HĐ', 0))}")
                                 st.write(f"**Hết HĐ:** {fmt_date(r['Ngày hết HĐ'])}")
                    if not df_kh.empty:
                        st.warning(f"🟡 {len(df_kh)} Khách sắp out")
                        for _, r in df_kh.iterrows(): 
                            days_left = (r['Ngày out'] - today).days
                            toa_nha = str(r.get('Toà', 'Chưa rõ')).strip()
                            with st.expander(f"🚪 {toa_nha} - P.{r['Mã căn']} (Còn {days_left} ngày)"):
                                st.write(f"**Khách:** {r.get('Tên khách thuê', 'N/A')}")
                                st.write(f"**Cọc trả:** {fmt_vnd(r.get('KH cọc', 0))}")
                                st.write(f"**Out:** {fmt_date(r['Ngày out'])}")
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
            "💰 Quản Lý Chi Phí", "📊 P&L (Lợi Nhuận)", "💸 Dòng Tiền",
            "📅 Quyết Toán" 
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
                    new = pd.DataFrame([{"Mã căn": str(can).strip(), "Loại": loai, "Tiền": tien, "Ngày": pd.to_datetime(d), "Chỉ số đồng hồ": ""}])
                    save_data(pd.concat([df_cp, new], ignore_index=True), "CHI_PHI"); time.sleep(1); st.rerun()
            
            df_cp_show = df_cp.copy()
            df_cp_show["Tiền"] = df_cp_show["Tiền"].apply(fmt_vnd)
            st.dataframe(df_cp_show, use_container_width=True, column_config={"Ngày": st.column_config.DateColumn(format="DD/MM/YY")})

        with tabs[3]:
            st.subheader("📋 Dữ Liệu Gốc")
            st.info("💡 Bạn có thể sửa trực tiếp trên bảng và bấm Lưu.")
            
            df_edit = df_main.copy()
            # Hiển thị số để người dùng dễ nhìn, nhưng khi lưu sẽ gỡ ra
            for c in COLS_MONEY:
                 if c in df_edit.columns: df_edit[c] = df_edit[c].apply(lambda x: "{:,.0f}".format(x).replace(",", "."))
            
            edited_df = st.data_editor(
                df_edit, 
                use_container_width=True,
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
            st.info("Xem Cảnh Báo Chi Tiết ở thanh Sidebar bên trái.")

        with tabs[5]:
            st.subheader("💰 Quản Lý Chi Phí & Doanh Thu")
            if not df_main.empty:
                df_agg = gop_du_lieu_phong(df_main)
                cols_show = ["Toà", "Mã căn", "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Giá", "KH thanh toán", "KH cọc", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân", "Ghi chú"]
                cols_exist = [c for c in cols_show if c in df_agg.columns]
                df_view = df_agg[cols_exist].copy()
                
                df_export_6 = df_view.copy() 
                for c in COLS_MONEY: 
                    if c in df_view.columns: df_view[c] = df_view[c].apply(fmt_vnd)
                
                st.dataframe(df_view.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}), use_container_width=True, column_config={"Ghi chú": st.column_config.TextColumn(width=500)})
                st.download_button("📥 Tải Excel", convert_df_to_excel(df_export_6), "QuanLyChiPhi.xlsx")

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
                for c in ["Doanh thu", "Giá vốn", "Chi phí Sale", "Lợi nhuận"]: df_show[c] = df_show[c].apply(fmt_vnd)

                st.dataframe(df_show.style.applymap(lambda x: 'color: red' if "(" in str(x) else '', subset=['Lợi nhuận']), use_container_width=True, column_config={"Ghi chú": st.column_config.TextColumn(width=500)})

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
                for c in ["Thu", "Chi", "Chi phí VH", "Ròng"]: df_cf_show[c] = df_cf_show[c].apply(fmt_vnd)
                st.dataframe(df_cf_show.style.applymap(lambda x: 'color: red' if "(" in str(x) else '', subset=['Ròng']), use_container_width=True, column_config={"Ghi chú": st.column_config.TextColumn(width=500)})

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
                        if row['Ngày ký'] <= end_date_mo and row['Ngày hết HĐ'] >= start_date_mo: cost_month = row['Giá HĐ']
                    
                    rev_month = 0
                    if pd.notna(row['Ngày in']) and pd.notna(row['Ngày out']):
                        if row['Ngày in'] <= end_date_mo and row['Ngày out'] >= start_date_mo: rev_month = row['Giá']
                    
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
                    for c in ["Doanh thu tháng", "Chi phí thuê (Vốn)", "Thuế phải đóng", "Lợi nhuận ròng"]: df_display[c] = df_display[c].apply(fmt_vnd)
                    st.dataframe(df_display.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}), use_container_width=True, column_config={"Ghi chú": st.column_config.TextColumn(width=300)})
                else: st.warning(f"Không có dữ liệu trong tháng {q_month}/{q_year}")

else:
    st.warning("👈 Vui lòng tải file **JSON Chìa Khóa** từ Google lên đây để bắt đầu.")