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

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================

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

# ==============================================================================
# 2. GIAO DIỆN & KẾT NỐI
# ==============================================================================

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

# ==============================================================================
# 3. LOGIC XỬ LÝ DỮ LIỆU
# ==============================================================================

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

        # --- HÀM GỘP DỮ LIỆU & TẠO GHI CHÚ ---
        def gop_du_lieu_phong(df_input):
            if df_input.empty: return df_input
            df = df_input.copy()
            
            # Tạo ghi chú chi tiết cho từng dòng
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
                'Giá HĐ': 'max', 'Giá': 'max',
                'TT cho chủ nhà': 'sum', 'Cọc cho chủ nhà': 'sum',
                'KH thanh toán': 'sum', 'KH cọc': 'sum',
                'Công ty': 'sum', 'Cá Nhân': 'sum',
                'SALE THẢO': 'sum', 'SALE NGA': 'sum', 'SALE LINH': 'sum',
                'Tên khách thuê': 'first',
                # Nối chuỗi ghi chú xuống dòng
                '_chi_tiet_nhap': lambda x: '\n'.join([f"• Nhập lần {i+1}: {v}" for i, v in enumerate(x) if v != "Trống"])
            }
            
            final_agg = {k: v for k, v in agg_rules.items() if k in df.columns}
            cols_group = ['Toà', 'Mã căn']
            if not all(col in df.columns for col in cols_group): return df

            df_grouped = df.groupby(cols_group, as_index=False).agg(final_agg)
            df_grouped = df_grouped.rename(columns={'_chi_tiet_nhap': 'Ghi chú'})
            return df_grouped

        # --- LOAD & CLEAN DATA ---
        df_main = load_data("HOP_DONG")
        df_cp = load_data("CHI_PHI")

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

        # --- SIDEBAR ---
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
                             d = (r['Ngày hết HĐ']-today).days
                             msg = "Đã hết hạn" if d < 0 else f"Còn {d} ngày"
                             st.caption(f"🏠 {r['Mã căn']}: {msg}")
                    if not df_kh.empty:
                        st.warning(f"🟡 {len(df_kh)} Khách sắp out")
            st.divider()
            if st.button("🔄 Tải lại dữ liệu (F5)", use_container_width=True): 
                st.cache_data.clear()
                st.rerun()

        DANH_SACH_NHA = { "Tòa A": ["A101"], "Tòa B": ["B101"], "Khác": [] }

        # ==============================================================================
        # 4. GIAO DIỆN CHÍNH (TABS)
        # ==============================================================================
        tabs = st.tabs([
            "✍️ Nhập Liệu", "📥 Upload Excel", "💸 Chi Phí Nội Bộ", 
            "📋 Dữ Liệu Gốc", "🏠 Cảnh Báo", 
            "💰 Quản Lý Chi Phí", "📊 P&L (Lợi Nhuận)", "💸 Dòng Tiền" 
        ])

        # --- TAB 1, 2, 3, 4, 5 (GIỮ NGUYÊN) ---
        with tabs[0]:
            st.subheader("✍️ Nhập Liệu Hợp Đồng Mới")
            with st.expander("🛠️ Công cụ hỗ trợ (Zalo / Hình ảnh)", expanded=False):
                c_txt, c_img = st.columns(2)
                with c_txt:
                    txt = st.text_area("Dán tin nhắn Zalo vào đây:")
                    if st.button("Phân tích Text"): st.session_state['auto'] = parse_text_message(txt)
                with c_img:
                    key_vis = st.text_input("API Key (Vision)", type="password", key="key_vis")
                    up = st.file_uploader("Upload ảnh hợp đồng", type=["jpg", "png"])
                    if up and key_vis and st.button("Phân tích Ảnh"):
                        with st.spinner("AI đang đọc..."): st.session_state['auto'] = parse_image_gemini(key_vis, Image.open(up))
            st.divider()
            av = st.session_state.get('auto', {}) 
            with st.form("main_form"):
                st.write("#### 1. Thông tin Phòng")
                c1_1, c1_2, c1_3, c1_4 = st.columns(4)
                with c1_1:
                    ds_toa = list(DANH_SACH_NHA.keys())
                    idx_toa = 0
                    if av.get("toa_nha") in ds_toa: idx_toa = ds_toa.index(av.get("toa_nha"))
                    chon_toa = st.selectbox("Chọn Tòa nhà", ds_toa, index=idx_toa)
                with c1_2:
                    ds_phong = DANH_SACH_NHA.get(chon_toa, [])
                    if not ds_phong: chon_can = st.text_input("Nhập Mã căn", value=str(av.get("ma_can","")))
                    else: chon_can = st.selectbox("Chọn Mã căn", ds_phong)
                with c1_3: chu_nha_sale = st.text_input("Chủ nhà - Sale")
                with c1_4: gia_thue = st.number_input("Giá thuê khách trả", min_value=0, step=100000, value=int(av.get("gia_thue", 0) or 0))

                st.write("#### 2. Thời gian & Hợp đồng")
                c2_1, c2_2, c2_3, c2_4 = st.columns(4)
                with c2_1: ngay_ky = st.date_input("Ngày ký HĐ", date.today())
                with c2_2:
                    thoi_han = st.selectbox("Thời hạn thuê", [6, 12, 1, 3, 24], format_func=lambda x: f"{x} tháng")
                    try: ngay_het_han_auto = ngay_ky + pd.Timedelta(days=thoi_han*30)
                    except: ngay_het_han_auto = ngay_ky
                    ngay_het_hd = st.date_input("Ngày hết HĐ (Tự động tính)", value=ngay_het_han_auto)
                with c2_3: ngay_in = st.date_input("Ngày khách vào (Check-in)", ngay_ky)
                with c2_4: ngay_out = st.date_input("Ngày khách ra (Check-out)", ngay_het_hd)

                st.write("#### 3. Thông tin Khách & Thanh toán")
                c3_1, c3_2, c3_3, c3_4 = st.columns(4)
                with c3_1: ten_khach = st.text_input("Tên khách thuê", value=str(av.get("ten_khach","")))
                with c3_2: gia_hd = st.number_input("Giá HĐ (Giá gốc)", min_value=0, step=100000)
                with c3_3: kh_coc = st.number_input("Khách cọc", min_value=0, step=100000)
                with c3_4: tt_chu_nha = st.text_input("TT cho chủ nhà (Ghi chú)")

                st.write("#### 4. Hoa hồng & Phí môi giới")
                c4_1, c4_2, c4_3, c4_4 = st.columns(4)
                with c4_1: sale_thao = st.number_input("Sale Thảo", min_value=0, step=50000)
                with c4_2: sale_nga = st.number_input("Sale Nga", min_value=0, step=50000)
                with c4_3: sale_linh = st.number_input("Sale Linh", min_value=0, step=50000)
                with c4_4: cong_ty = st.number_input("Công ty giữ", min_value=0, step=50000)

                if st.form_submit_button("💾 LƯU HỢP ĐỒNG MỚI", type="primary"):
                    new_data = {
                        "Tòa nhà": chon_toa, "Mã căn": chon_can, "Toà": chon_toa,
                        "Chủ nhà - sale": chu_nha_sale, "Ngày ký": pd.to_datetime(ngay_ky),
                        "Ngày hết HĐ": pd.to_datetime(ngay_het_hd), "Giá HĐ": gia_hd,
                        "TT cho chủ nhà": tt_chu_nha, "Tên khách thuê": ten_khach,
                        "Ngày in": pd.to_datetime(ngay_in), "Ngày out": pd.to_datetime(ngay_out),
                        "Giá": gia_thue, "KH cọc": kh_coc, "Công ty": cong_ty,
                        "SALE THẢO": sale_thao, "SALE NGA": sale_nga, "SALE LINH": sale_linh,
                        "Cọc cho chủ nhà": "", "KH thanh toán": "", "Cá Nhân": "", "Hết hạn khách hàng": "", "Ráp khách khi hết hạn": ""
                    }
                    new_row = pd.DataFrame([new_data])
                    df_final = pd.concat([df_main, new_row], ignore_index=True)
                    save_data(df_final, "HOP_DONG")
                    st.session_state['auto'] = {}
                    time.sleep(1); st.rerun()

        with tabs[1]:
            st.header("📤 Quản lý File Excel")
            st.download_button("📥 Tải File Mẫu Hợp Đồng (.xlsx)", convert_df_to_excel(pd.DataFrame(columns=COLUMNS)), "mau_hop_dong.xlsx")
            st.divider()
            up = st.file_uploader("Chọn file Excel từ máy tính", type=["xlsx"], key="up_main")
            if up is not None:
                try:
                    df_up = pd.read_excel(up)
                    st.write(f"✅ Đã đọc được file: {len(df_up)} dòng.")
                    if st.button("🚀 BẮT ĐẦU ĐỒNG BỘ LÊN CLOUD", type="primary"):
                        with st.spinner("Đang xử lý..."):
                            for col in COLUMNS:
                                if col not in df_up.columns: df_up[col] = ""
                            df_up = df_up[COLUMNS]
                            for col in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                                if col in df_up.columns:
                                    df_up[col] = pd.to_datetime(df_up[col], errors='coerce').dt.strftime('%Y-%m-%d')
                            save_data(df_up, "HOP_DONG"); time.sleep(2); st.rerun()
                except Exception as e: st.error(f"❌ File Excel bị lỗi: {e}")

        with tabs[2]:
            st.subheader("💸 Quản Lý Chi Phí Nội Bộ")
            with st.expander("🧮 Máy tính & Thêm Mới Thủ Công", expanded=True):
                col_calc1, col_calc2, col_calc3, col_calc4 = st.columns(4)
                sc = col_calc1.number_input("Số cũ", 0.0); sm = col_calc2.number_input("Số mới", 0.0); dg = col_calc3.number_input("Đơn giá", 3500)
                col_calc4.metric("Thành tiền", f"{(sm-sc)*dg:,.0f}")
                st.divider()
                with st.form("cp_form"):
                    c1, c2, c3, c4, c5 = st.columns(5)
                    d = c1.date_input("Ngày", date.today()); can = c2.text_input("Mã căn")
                    loai = c3.selectbox("Loại", ["Điện", "Nước", "Net", "Dọn dẹp", "Khác"])
                    tien = c4.number_input("Tiền", value=float((sm-sc)*dg)); chi_so = c5.text_input("Chỉ số đồng hồ", placeholder="VD: 1200 - 1350")
                    if st.form_submit_button("Lưu Chi Phí"):
                        new = pd.DataFrame([{"Mã căn": str(can).strip(), "Loại": loai, "Tiền": tien, "Ngày": pd.to_datetime(d), "Chỉ số đồng hồ": chi_so}])
                        df_cp_new = pd.concat([df_cp, new], ignore_index=True); save_data(df_cp_new, "CHI_PHI"); time.sleep(1); st.rerun()
            st.divider()
            up_cp = st.file_uploader("Chọn file Excel chi phí", type=["xlsx"], key="up_cp")
            if up_cp is not None:
                try:
                    df_up_cp = pd.read_excel(up_cp)
                    if st.button("🚀 ĐỒNG BỘ CHI PHÍ"):
                        with st.spinner("Đang đồng bộ..."):
                            if "Chỉ số đồng hồ" not in df_up_cp.columns: df_up_cp["Chỉ số đồng hồ"] = ""
                            df_up_cp = df_up_cp[COLUMNS_CP]
                            if "Ngày" in df_up_cp.columns: df_up_cp["Ngày"] = pd.to_datetime(df_up_cp["Ngày"], errors='coerce')
                            df_combined = pd.concat([df_cp, df_up_cp], ignore_index=True)
                            df_final_cp = df_combined.drop_duplicates(subset=['Ngày', 'Mã căn', 'Loại', 'Tiền'], keep='last')
                            save_data(df_final_cp, "CHI_PHI"); time.sleep(1); st.rerun()
                except Exception as e: st.error(f"❌ Lỗi file: {e}")
            st.divider()
            if st.button("🧹 Quét & Xóa Dữ Liệu Trùng Lặp", type="secondary"):
                if not df_cp.empty:
                    df_clean = df_cp.drop_duplicates(subset=['Ngày', 'Mã căn', 'Loại', 'Tiền'], keep='first')
                    if len(df_clean) < len(df_cp): save_data(df_clean, "CHI_PHI"); st.success(f"✅ Đã xóa trùng!"); time.sleep(1); st.rerun()
                    else: st.info("👍 Dữ liệu sạch.")
            edited_cp = st.data_editor(df_cp, num_rows="dynamic", use_container_width=True, column_config={"Ngày": st.column_config.DateColumn(format="DD/MM/YYYY"), "Tiền": st.column_config.NumberColumn(format="%d"), "Mã căn": st.column_config.TextColumn(), "Chỉ số đồng hồ": st.column_config.TextColumn(width="medium")})
            if st.button("💾 LƯU LÊN ĐÁM MÂY (CHI PHÍ)", type="primary"): save_data(edited_cp, "CHI_PHI"); time.sleep(1); st.rerun()

        with tabs[3]:
            st.subheader("📋 Dữ Liệu Hợp Đồng (Gốc)")
            search_term = st.text_input("🔍 Tìm kiếm nhanh:")
            df_show = df_main
            if search_term and not df_show.empty:
                df_show = df_show[df_show.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)]
                st.success(f"🔎 Tìm thấy {len(df_show)} kết quả.")
            edited_df = st.data_editor(df_show, num_rows="dynamic", use_container_width=True, column_config={"Ngày ký": st.column_config.DateColumn(format="DD/MM/YYYY"), "Ngày hết HĐ": st.column_config.DateColumn(format="DD/MM/YYYY"), "Ngày in": st.column_config.DateColumn(format="DD/MM/YYYY"), "Ngày out": st.column_config.DateColumn(format="DD/MM/YYYY"), "Giá": st.column_config.NumberColumn(format="%d"), "Mã căn": st.column_config.TextColumn()})
            if st.button("💾 LƯU LÊN ĐÁM MÂY (HỢP ĐỒNG)", type="primary"): save_data(edited_df, "HOP_DONG"); time.sleep(1); st.rerun()

        with tabs[4]:
            st.subheader("🏠 Trung Tâm Cảnh Báo")
            if not df_main.empty:
                df_alert = df_main.sort_values('Ngày out').groupby(['Mã căn', 'Toà']).tail(1).copy()
                st.write("#### 1️⃣ Cảnh báo Hết Hạn Hợp Đồng")
                def check_hd(row):
                    x = row['Ngày hết HĐ']
                    if pd.isna(x): return "N/A"
                    days = (x - today).days
                    if days < 0: return "Hết hạn"
                    if days <= 30: return "Sắp hết"
                    return "Còn hạn"
                df_warning_hd = df_alert[df_alert.apply(lambda r: check_hd(r) in ["Hết hạn", "Sắp hết"], axis=1)]
                if df_warning_hd.empty: st.success("✅ Không có HĐ sắp hết hạn.")
                else:
                    for idx, row in df_warning_hd.iterrows():
                        days = (row['Ngày hết HĐ'] - today).days
                        status = "ĐÃ QUÁ HẠN" if days < 0 else f"Còn {days} ngày"
                        with st.expander(f"🔴 {row['Mã căn']} - {row['Tên khách thuê']} ({status})"):
                            st.write(f"📅 Ngày hết HĐ: {row['Ngày hết HĐ'].strftime('%d/%m/%Y')}")
                            st.code(f"Chào bạn {row['Tên khách thuê']},\nHợp đồng phòng {row['Mã căn']} sắp hết hạn vào ngày {row['Ngày hết HĐ'].strftime('%d/%m/%Y')}. Vui lòng liên hệ để gia hạn.", language=None)
                st.divider(); st.write("#### 2️⃣ Cảnh báo Khách Sắp Trả Phòng (Check-out)")
                def check_out(row):
                    x = row['Ngày out']
                    if pd.isna(x): return "N/A"
                    days = (x - today).days
                    if 0 <= days <= 7: return "Sắp out"
                    return "Còn ở"
                df_warning_out = df_alert[df_alert.apply(lambda r: check_out(r) == "Sắp out", axis=1)]
                if df_warning_out.empty: st.success("✅ Không có phòng sắp trả.")
                else:
                    for idx, row in df_warning_out.iterrows():
                        days = (row['Ngày out'] - today).days
                        with st.expander(f"🚪 {row['Mã căn']} - {row['Tên khách thuê']} (Còn {days} ngày)"):
                            st.write(f"📅 Trả phòng: {row['Ngày out'].strftime('%d/%m/%Y')}")
                            st.code(f"Chào bạn {row['Tên khách thuê']},\nPhòng {row['Mã căn']} đến hạn trả vào {row['Ngày out'].strftime('%d/%m/%Y')}. Vui lòng vệ sinh và bàn giao.", language=None)

        # ----------------------------------------------------------------------
        # TAB 6: QUẢN LÝ CHI PHÍ (DETAIL VIEW) - CỘT GHI CHÚ RỘNG
        # ----------------------------------------------------------------------
        with tabs[5]:
            st.subheader("💰 Quản Lý Chi Phí & Doanh Thu Chi Tiết")
            if not df_main.empty:
                df_agg = gop_du_lieu_phong(df_main)
                
                cols_to_show = ["Toà", "Mã căn", "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà", "Giá", "KH thanh toán", "KH cọc", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân", "Ghi chú"]
                existing_cols = [c for c in cols_to_show if c in df_agg.columns]
                
                df_view = df_agg[existing_cols].copy()
                df_view = df_view.rename(columns={"TT cho chủ nhà": "Thanh toán HĐ", "Cọc cho chủ nhà": "Cọc HĐ", "Giá": "Giá thuê", "KH thanh toán": "Khách thanh toán", "KH cọc": "Khách cọc", "Công ty": "HH Công ty", "Cá Nhân": "HH Cá nhân"})
                if "Mã căn" in df_view.columns: df_view = df_view.sort_values(by=["Toà", "Mã căn"])
                
                numeric_cols = ["Giá HĐ", "Thanh toán HĐ", "Cọc HĐ", "Giá thuê", "Khách thanh toán", "Khách cọc", "SALE THẢO", "SALE NGA", "SALE LINH", "HH Công ty", "HH Cá nhân"]
                total_row = pd.DataFrame(df_view[numeric_cols].sum(numeric_only=True)).T; total_row["Toà"] = "TỔNG CỘNG"; total_row = total_row.fillna("")
                df_final_view = pd.concat([df_view, total_row], ignore_index=True)
                for col in numeric_cols: 
                    if col in df_final_view.columns: df_final_view[col] = df_final_view[col].apply(fmt_vnd)
                
                st.dataframe(
                    df_final_view.style.set_properties(**{'border-color': 'lightgrey', 'border-style': 'solid', 'border-width': '1px'}),
                    use_container_width=True, 
                    # 💡 TĂNG ĐỘ RỘNG CỘT GHI CHÚ
                    column_config={"Ghi chú": st.column_config.TextColumn(width=500, help="Di chuột để xem chi tiết hoặc dùng công cụ soi bên dưới")}
                )

                # --- 🔎 CÔNG CỤ SOI GHI CHÚ ---
                st.divider()
                st.write("##### 🔎 Soi Chi Tiết Ghi Chú")
                c_sel, c_view = st.columns([1, 3])
                with c_sel:
                    # Tạo danh sách phòng để chọn
                    list_phong = df_view[df_view['Mã căn'].notna()]['Mã căn'].unique().tolist()
                    phong_chon = st.selectbox("Chọn Phòng để xem:", list_phong, key="sel_tab6")
                with c_view:
                    if phong_chon:
                        # Lấy ghi chú của phòng đó
                        note_content = df_view[df_view['Mã căn'] == phong_chon]['Ghi chú'].values[0]
                        st.text_area("Nội dung ghi chú đầy đủ:", value=note_content, height=150)

            else: st.info("Chưa có dữ liệu.")

        # ----------------------------------------------------------------------
        # TAB 7: TỔNG HỢP CHI PHÍ (P&L) - ĐÃ GỘP DÒNG - CÓ SOI GHI CHÚ
        # ----------------------------------------------------------------------
        with tabs[6]:
            st.subheader("📊 Báo Cáo Lợi Nhuận (Profit & Loss)")
            c_filter1, c_filter2 = st.columns(2)
            with c_filter1: sel_month = st.selectbox("Chọn Tháng", range(1, 13), index=date.today().month - 1, key="pl_month")
            with c_filter2: sel_year = st.number_input("Chọn Năm", min_value=2020, max_value=2030, value=date.today().year, key="pl_year")
            st.divider()

            if not df_main.empty:
                df_merged = gop_du_lieu_phong(df_main)
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

                # Cập nhật Ghi chú thông minh (Kèm nội dung nhập liệu cũ)
                def make_smart_note(row, profit, cogs):
                    original_note = str(row.get('Ghi chú', ''))
                    warn = []
                    if cogs == 0 and profit == 0: warn.append("⚠️ Thiếu ngày")
                    elif profit < 0: warn.append("📉 Lỗ")
                    
                    if warn: return " | ".join(warn) + "\n" + original_note
                    return original_note

                df_calc["Ghi chú Smart"] = df_calc.apply(lambda r: make_smart_note(r, r['Lợi nhuận ròng'], r['Chi phí vốn (theo khách)']), axis=1)
                
                cols_final = ["Toà", "Mã căn", "Tổng giá trị HĐ", "Chi phí vốn (theo khách)", "Doanh thu cho thuê", "Tổng Chi Phí Sale", "Công ty", "Cá Nhân", "Lợi nhuận ròng", "Ghi chú Smart"]
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
                    # 💡 TĂNG ĐỘ RỘNG CỘT GHI CHÚ
                    column_config={"Ghi chú Smart": st.column_config.TextColumn("Ghi chú & Cảnh báo", width=500)}
                )

                # --- 🔎 CÔNG CỤ SOI GHI CHÚ ---
                st.divider()
                st.write("##### 🔎 Soi Chi Tiết Ghi Chú")
                c_sel_pl, c_view_pl = st.columns([1, 3])
                with c_sel_pl:
                    list_phong_pl = df_calc[df_calc['Mã căn'].notna()]['Mã căn'].unique().tolist()
                    phong_chon_pl = st.selectbox("Chọn Phòng:", list_phong_pl, key="sel_tab7")
                with c_view_pl:
                    if phong_chon_pl:
                        note_pl = df_calc[df_calc['Mã căn'] == phong_chon_pl]['Ghi chú Smart'].values[0]
                        st.info(note_pl) # Dùng st.info để hiển thị nổi bật

            else: st.info("Chưa có dữ liệu.")

        # ----------------------------------------------------------------------
        # TAB 8: QUẢN LÝ DÒNG TIỀN - CÓ SOI GHI CHÚ
        # ----------------------------------------------------------------------
        with tabs[7]:
            st.subheader("💸 Quản Lý Dòng Tiền (Thực Thu - Thực Chi)")
            if not df_main.empty:
                df_cf = gop_du_lieu_phong(df_main)
                
                df_cf['Thu: Thanh toán'] = df_cf['KH thanh toán']
                df_cf['Thu: Cọc'] = df_cf['KH cọc']
                df_cf['TỔNG THU'] = df_cf['Thu: Thanh toán'] + df_cf['Thu: Cọc']
                
                df_cf['Chi: Chủ nhà'] = df_cf['TT cho chủ nhà'] + df_cf['Cọc cho chủ nhà']
                df_cf['Chi: Hoa hồng'] = df_cf['SALE THẢO'] + df_cf['SALE NGA'] + df_cf['SALE LINH'] + df_cf['Công ty'] + df_cf['Cá Nhân']
                
                df_op_cost = pd.DataFrame()
                if not df_cp.empty:
                    df_op_cost = df_cp.groupby("Mã căn")["Tiền"].sum().reset_index()
                    df_op_cost.columns = ["Mã căn", "Chi: Vận hành"]
                
                df_final_cf = pd.merge(df_cf, df_op_cost, on="Mã căn", how="left").fillna(0)
                df_final_cf['TỔNG CHI'] = df_final_cf['Chi: Chủ nhà'] + df_final_cf['Chi: Hoa hồng'] + df_final_cf['Chi: Vận hành']
                df_final_cf['DÒNG TIỀN RÒNG'] = df_final_cf['TỔNG THU'] - df_final_cf['TỔNG CHI']
                
                c_cf1, c_cf2, c_cf3 = st.columns(3)
                tot_in = df_final_cf['TỔNG THU'].sum()
                tot_out = df_final_cf['TỔNG CHI'].sum()
                net_cf = tot_in - tot_out
                c_cf1.metric("💰 TỔNG THỰC THU", fmt_vnd(tot_in))
                c_cf2.metric("💸 TỔNG THỰC CHI", fmt_vnd(tot_out))
                c_cf3.metric("💎 DÒNG TIỀN RÒNG", fmt_vnd(net_cf), delta_color="normal" if net_cf > 0 else "inverse")
                st.divider()

                def explain_cf(row):
                    original_note = str(row.get('Ghi chú', ''))
                    net = row['DÒNG TIỀN RÒNG']
                    status = "✅ Dương" if net >= 0 else "⚠️ Âm"
                    return f"[{status}] {original_note}"

                df_final_cf['Ghi chú CF'] = df_final_cf.apply(explain_cf, axis=1)

                cols_cf_show = ["Toà", "Mã căn", "Thu: Thanh toán", "Thu: Cọc", "TỔNG THU", "Chi: Chủ nhà", "Chi: Hoa hồng", "Chi: Vận hành", "TỔNG CHI", "DÒNG TIỀN RÒNG", "Ghi chú CF"]
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
                    # 💡 TĂNG ĐỘ RỘNG CỘT GHI CHÚ
                    column_config={"Ghi chú CF": st.column_config.TextColumn("Chi tiết & Ghi chú", width=500)}
                )

                # --- 🔎 CÔNG CỤ SOI GHI CHÚ ---
                st.divider()
                st.write("##### 🔎 Soi Chi Tiết Ghi Chú")
                c_sel_cf, c_view_cf = st.columns([1, 3])
                with c_sel_cf:
                    list_phong_cf = df_final_cf[df_final_cf['Mã căn'].notna()]['Mã căn'].unique().tolist()
                    phong_chon_cf = st.selectbox("Chọn Phòng:", list_phong_cf, key="sel_tab8")
                with c_view_cf:
                    if phong_chon_cf:
                        note_cf = df_final_cf[df_final_cf['Mã căn'] == phong_chon_cf]['Ghi chú CF'].values[0]
                        st.info(note_cf)

            else: st.info("Chưa có dữ liệu.")

else:
    st.warning("👈 Vui lòng tải file **JSON Chìa Khóa** từ Google lên đây để bắt đầu.")