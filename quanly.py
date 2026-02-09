import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, datetime, timedelta
import os
import json
import re
import time
import io
from PIL import Image

# --- THƯ VIỆN KẾT NỐI ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. CẤU HÌNH ---
st.set_page_config(page_title="MT60 Cloud", layout="wide", page_icon="☁️")

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

# --- 2. GIAO DIỆN CHÍNH ---
st.title("☁️ MT60 STUDIO - ONLINE")

# --- 3. KHU VỰC ĐĂNG NHẬP ---
st.sidebar.header("🔐 Đăng Nhập")

# Nút upload
uploaded_key = st.sidebar.file_uploader("Chọn file JSON để mở khóa", type=['json'])

# --- 4. HÀM KẾT NỐI ---
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
        st.error(f"❌ Lỗi: {e}")
        return None

# --- 5. LOGIC CHẠY APP ---
if uploaded_key is not None:
    uploaded_key.seek(0)
    
    with st.spinner("Đang kết nối..."):
        sh = connect_google_sheet(uploaded_key)
    
    if sh:
        st.sidebar.success("✅ Đã kết nối!")
        
        # --- CÁC HÀM XỬ LÝ DỮ LIỆU ---
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
            except Exception as e:
                st.error(f"❌ Lỗi khi lưu: {e}")

        def to_num(val):
            if isinstance(val, str): 
                val = val.replace(',', '').replace('.', '').strip()
                if val == '' or val.lower() == 'nan': return 0
            try: return float(val)
            except: return 0

        # --- HÀM ĐỊNH DẠNG TIỀN VIỆT NAM ---
        def fmt_vnd(val):
            try:
                if pd.isna(val) or val == "": return "-"
                val = float(val)
                if val < 0: return "({:,.0f})".format(abs(val)).replace(",", ".")
                return "{:,.0f}".format(val).replace(",", ".")
            except:
                return str(val)

        def convert_df_to_excel(df):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            return output.getvalue()
        
        def format_date_vn(df):
            df_fmt = df.copy()
            for col in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out", "Ngày"]:
                if col in df_fmt.columns:
                    df_fmt[col] = pd.to_datetime(df_fmt[col], errors='coerce').dt.strftime('%d/%m/%y').replace('NaT', '')
            return df_fmt
        
        def check_ai_ready(): return AI_AVAILABLE

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
            if not check_ai_ready(): return None
            try:
                client = genai.Client(api_key=api_key)
                prompt = """Trích xuất JSON: {"ma_can": "số phòng", "ten_khach": "tên", "gia_thue": số_nguyên, "ngay_in": "YYYY-MM-DD", "ngay_out": "YYYY-MM-DD"}"""
                try: response = client.models.generate_content(model="gemini-1.5-flash", contents=[prompt, image])
                except: response = client.models.generate_content(model="gemini-1.5-pro", contents=[prompt, image])
                return json.loads(response.text.replace("```json", "").replace("```", "").strip())
            except: return None

        # --- LOAD DỮ LIỆU ---
        df_main = load_data("HOP_DONG")
        df_cp = load_data("CHI_PHI")

        # --- XỬ LÝ DỮ LIỆU CHI PHÍ ---
        if df_cp.empty:
            df_cp = pd.DataFrame(columns=COLUMNS_CP)
            df_cp["Ngày"] = pd.Series(dtype='datetime64[ns]')
            df_cp["Tiền"] = pd.Series(dtype='float')
        else:
            if "Chỉ số đồng hồ" not in df_cp.columns: df_cp["Chỉ số đồng hồ"] = ""
            if "Ngày" in df_cp.columns:
                df_cp["Ngày"] = pd.to_datetime(df_cp["Ngày"], errors='coerce')
            if "Tiền" in df_cp.columns:
                df_cp["Tiền"] = pd.to_numeric(df_cp["Tiền"], errors='coerce').fillna(0)
            df_cp["Mã căn"] = df_cp["Mã căn"].astype(str)
            df_cp["Loại"] = df_cp["Loại"].astype(str)
            df_cp["Chỉ số đồng hồ"] = df_cp["Chỉ số đồng hồ"].astype(str)

        # Xử lý bảng Hợp đồng
        if not df_main.empty:
            if "Mã căn" in df_main.columns: df_main["Mã căn"] = df_main["Mã căn"].astype(str)
            for c in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                if c in df_main.columns: df_main[c] = pd.to_datetime(df_main[c], errors='coerce')
            
            cols_to_numeric = [
                "Giá", "Giá HĐ", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân",
                "TT cho chủ nhà", "Cọc cho chủ nhà", "KH thanh toán", "KH cọc"
            ]
            for c in cols_to_numeric:
                if c in df_main.columns: df_main[c] = df_main[c].apply(to_num)

        # --- SIDEBAR THÔNG BÁO ---
        with st.sidebar:
            st.divider()
            st.header("🔔 Thông Báo")
            today = pd.Timestamp(date.today())
            if not df_main.empty:
                df_active = df_main.sort_values('Ngày out').groupby(['Mã căn', 'Toà']).tail(1).copy()
                df_hd = df_active[(df_active['Ngày hết HĐ'].notna()) & ((df_active['Ngày hết HĐ'] - today).dt.days.between(-999, 30))]
                df_kh = df_active[(df_active['Ngày out'].notna()) & ((df_active['Ngày out'] - today).dt.days.between(0, 7))]

                if df_hd.empty and df_kh.empty: st.success("✅ Mọi thứ ổn định")
                else:
                    if not df_hd.empty:
                        st.error(f"🔴 {len(df_hd)} HĐ cần xử lý")
                        for _, r in df_hd.iterrows():
                             d = (r['Ngày hết HĐ']-today).days
                             msg = "Đã hết hạn" if d < 0 else f"Còn {d} ngày"
                             toa_info = f" ({r['Toà']})" if str(r['Toà']).strip() != '' else ''
                             st.caption(f"🏠 {r['Mã căn']}{toa_info}: {msg}")
                             
                    if not df_kh.empty:
                        st.warning(f"🟡 {len(df_kh)} Khách sắp out")
                        for _, r in df_kh.iterrows(): 
                            toa_info = f" ({r['Toà']})" if str(r['Toà']).strip() != '' else ''
                            st.caption(f"🚪 {r['Mã căn']}{toa_info}: {(r['Ngày out']-today).days} ngày")
            
            if st.button("🔄 Tải lại dữ liệu (F5)"): 
                st.cache_data.clear()
                st.rerun()

        # --- CẤU HÌNH DANH SÁCH TÒA VÀ PHÒNG ---
        DANH_SACH_NHA = {
            "Tòa A": ["A101", "A102", "A201", "A202", "A301", "A302"],
            "Tòa B": ["B101", "B102", "B201", "B202"],
            "Tòa C": ["C101", "C102", "C201", "C202"],
            "Khác": [] 
        }

        # --- CÁC TAB CHỨC NĂNG ---
        tabs = st.tabs([
            "✍️ Nhập Liệu Thủ Công", 
            "📥 Nhập Liệu Bằng Excel", 
            "💸 Chi Phí Nội Bộ",        
            "📋 Tổng Hợp Dữ Liệu",      
            "🏠 Cảnh Báo Phòng",        
            "💰 Quản Lý Chi Phí",      
            "📊 Tổng Hợp Chi Phí", 
            "💸 Quản Lý Dòng Tiền" 
        ])

        # --- TAB 1: NHẬP LIỆU THỦ CÔNG ---
        with tabs[0]:
            st.subheader("✍️ Nhập Liệu Hợp Đồng Mới")
            with st.expander("🛠️ Công cụ hỗ trợ (Zalo / Hình ảnh)", expanded=False):
                c_txt, c_img = st.columns(2)
                with c_txt:
                    txt = st.text_area("Dán tin nhắn Zalo vào đây:"); 
                    if st.button("Phân tích Text"): st.session_state['auto'] = parse_text_message(txt)
                with c_img:
                    key_vis = st.text_input("API Key (Vision - Nếu có)", type="password", key="key_vis")
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
                    if not ds_phong: 
                        chon_can = st.text_input("Nhập Mã căn", value=str(av.get("ma_can","")))
                    else:
                        chon_can = st.selectbox("Chọn Mã căn", ds_phong)
                with c1_3:
                    chu_nha_sale = st.text_input("Chủ nhà - Sale")
                with c1_4:
                    gia_thue = st.number_input("Giá thuê khách trả", min_value=0, step=100000, value=int(av.get("gia_thue", 0) or 0))

                st.write("#### 2. Thời gian & Hợp đồng")
                c2_1, c2_2, c2_3, c2_4 = st.columns(4)
                with c2_1:
                    ngay_ky = st.date_input("Ngày ký HĐ", date.today())
                with c2_2:
                    thoi_han = st.selectbox("Thời hạn thuê", [6, 12, 1, 3, 24], format_func=lambda x: f"{x} tháng")
                    try: ngay_het_han_auto = ngay_ky + pd.Timedelta(days=thoi_han*30)
                    except: ngay_het_han_auto = ngay_ky
                    ngay_het_hd = st.date_input("Ngày hết HĐ (Tự động tính)", value=ngay_het_han_auto)
                with c2_3:
                    ngay_in = st.date_input("Ngày khách vào (Check-in)", ngay_ky)
                with c2_4:
                    ngay_out = st.date_input("Ngày khách ra (Check-out)", ngay_het_hd)

                st.write("#### 3. Thông tin Khách & Thanh toán")
                c3_1, c3_2, c3_3, c3_4 = st.columns(4)
                with c3_1:
                    ten_khach = st.text_input("Tên khách thuê", value=str(av.get("ten_khach","")))
                with c3_2:
                    gia_hd = st.number_input("Giá HĐ (Giá gốc)", min_value=0, step=100000)
                with c3_3:
                    kh_coc = st.number_input("Khách cọc", min_value=0, step=100000)
                with c3_4:
                    tt_chu_nha = st.text_input("TT cho chủ nhà (Ghi chú)")

                st.write("#### 4. Hoa hồng & Phí môi giới")
                c4_1, c4_2, c4_3, c4_4 = st.columns(4)
                with c4_1:
                    sale_thao = st.number_input("Sale Thảo", min_value=0, step=50000)
                with c4_2:
                    sale_nga = st.number_input("Sale Nga", min_value=0, step=50000)
                with c4_3:
                    sale_linh = st.number_input("Sale Linh", min_value=0, step=50000)
                with c4_4:
                    cong_ty = st.number_input("Công ty giữ", min_value=0, step=50000)

                submitted = st.form_submit_button("💾 LƯU HỢP ĐỒNG MỚI", type="primary")
                if submitted:
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
                    save_data(df_final, "HOP_DONG"); st.session_state['auto'] = {}; time.sleep(1); st.rerun()

        # --- TAB 2: NHẬP LIỆU BẰNG EXCEL ---
        with tabs[1]:
            st.header("📤 Quản lý File Excel")
            st.subheader("Bước 1: Tải file mẫu chuẩn")
            df_mau = pd.DataFrame(columns=COLUMNS)
            st.download_button("📥 Tải File Mẫu Hợp Đồng (.xlsx)", convert_df_to_excel(df_mau), "mau_hop_dong.xlsx")
            st.divider()
            st.subheader("Bước 2: Upload dữ liệu")
            up = st.file_uploader("Chọn file Excel từ máy tính", type=["xlsx"], key="up_main")
            if up is not None:
                try:
                    df_up = pd.read_excel(up)
                    st.write(f"✅ Đã đọc được file: {len(df_up)} dòng.")
                    if st.button("🚀 BẮT ĐẦU ĐỒNG BỘ LÊN CLOUD", type="primary"):
                        with st.spinner("Đang xử lý và đồng bộ..."):
                            for col in COLUMNS:
                                if col not in df_up.columns: df_up[col] = ""
                            df_up = df_up[COLUMNS]
                            for col in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                                if col in df_up.columns:
                                    df_up[col] = pd.to_datetime(df_up[col], errors='coerce').dt.strftime('%Y-%m-%d')
                            save_data(df_up, "HOP_DONG")
                            time.sleep(2); st.rerun()
                except Exception as e:
                    st.error(f"❌ File Excel bị lỗi: {e}")

        # --- TAB 3: CHI PHÍ NỘI BỘ ---
        with tabs[2]:
            st.subheader("💸 Quản Lý Chi Phí Nội Bộ")
            
            with st.expander("🧮 Thêm mới Thủ Công", expanded=True):
                # Máy tính
                st.write("##### 🧮 Máy tính nhanh")
                c_cal1, c_cal2, c_cal3, c_cal4 = st.columns(4)
                sc = c_cal1.number_input("Số cũ", 0.0)
                sm = c_cal2.number_input("Số mới", 0.0)
                dg = c_cal3.number_input("Đơn giá", 3500)
                c_cal4.metric("Thành tiền", f"{(sm-sc)*dg:,.0f}")
                
                st.divider()
                st.write("##### 📝 Nhập phiếu chi")
                with st.form("cp_form"):
                    c1, c2, c3, c4, c5 = st.columns(5)
                    d = c1.date_input("Ngày", date.today())
                    can = c2.text_input("Mã căn")
                    loai = c3.selectbox("Loại", ["Điện", "Nước", "Net", "Dọn dẹp", "Khác"])
                    tien = c4.number_input("Tiền", value=float((sm-sc)*dg))
                    chi_so = c5.text_input("Chỉ số đồng hồ", placeholder="VD: 1200 - 1350")
                    
                    if st.form_submit_button("Lưu Chi Phí"):
                        new = pd.DataFrame([{
                            "Mã căn": str(can).strip(), "Loại": loai, "Tiền": tien, 
                            "Ngày": pd.to_datetime(d), "Chỉ số đồng hồ": chi_so
                        }])
                        df_cp_new = pd.concat([df_cp, new], ignore_index=True)
                        save_data(df_cp_new, "CHI_PHI"); time.sleep(1); st.rerun()

            st.divider()
            st.subheader("📤 Nhập Chi Phí Bằng Excel")
            
            # --- FILE MẪU ---
            df_mau_cp = pd.DataFrame(columns=COLUMNS_CP)
            df_mau_cp.loc[0] = ["2023-10-01", "A101", "Điện", 500000, "1200 - 1300"] # Dòng mẫu
            st.download_button("📥 Tải File Mẫu Chi Phí (.xlsx)", convert_df_to_excel(df_mau_cp), "mau_chi_phi.xlsx")
            
            # Upload file
            up_cp = st.file_uploader("Chọn file Excel chi phí từ máy tính", type=["xlsx"], key="up_cp")
            if up_cp is not None:
                try:
                    df_up_cp = pd.read_excel(up_cp)
                    st.write(f"✅ Đã đọc được file: {len(df_up_cp)} dòng.")
                    
                    # --- XEM TRƯỚC DỮ LIỆU ---
                    st.write("🔍 **Xem trước 5 dòng đầu:**")
                    st.dataframe(df_up_cp.head())
                    
                    # Kiểm tra cột
                    req_cols = ["Ngày", "Mã căn", "Loại", "Tiền"]
                    missing = [c for c in req_cols if c not in df_up_cp.columns]
                    
                    if missing:
                        st.error(f"❌ File thiếu cột: {', '.join(missing)}")
                    else:
                        if st.button("🚀 ĐỒNG BỘ CHI PHÍ LÊN CLOUD"):
                            with st.spinner("Đang đồng bộ..."):
                                if "Chỉ số đồng hồ" not in df_up_cp.columns: df_up_cp["Chỉ số đồng hồ"] = ""
                                df_up_cp = df_up_cp[COLUMNS_CP] # Sắp xếp cột
                                if "Ngày" in df_up_cp.columns:
                                    df_up_cp["Ngày"] = pd.to_datetime(df_up_cp["Ngày"], errors='coerce')
                                
                                # --- FIX LỖI TRÙNG LẶP ---
                                df_combined = pd.concat([df_cp, df_up_cp], ignore_index=True)
                                # Lọc bỏ dòng trùng (Ngày, Mã căn, Loại, Tiền giống nhau thì bỏ)
                                df_final_cp = df_combined.drop_duplicates(subset=['Ngày', 'Mã căn', 'Loại', 'Tiền'], keep='last')
                                
                                save_data(df_final_cp, "CHI_PHI")
                                time.sleep(1); st.rerun()
                except Exception as e:
                    st.error(f"❌ Lỗi file: {e}")

            st.divider()
            
            # --- TÍNH NĂNG MỚI: NÚT XÓA TRÙNG LẶP ---
            if st.button("🧹 Quét & Xóa Dữ Liệu Trùng Lặp", type="secondary"):
                if not df_cp.empty:
                    with st.spinner("Đang dọn dẹp..."):
                        old_len = len(df_cp)
                        df_clean = df_cp.drop_duplicates(subset=['Ngày', 'Mã căn', 'Loại', 'Tiền'], keep='first')
                        new_len = len(df_clean)
                        if new_len < old_len:
                            save_data(df_clean, "CHI_PHI")
                            st.success(f"✅ Đã xóa {old_len - new_len} dòng trùng lặp!")
                            time.sleep(1); st.rerun()
                        else:
                            st.info("👍 Dữ liệu sạch, không có dòng trùng.")

            # Bảng hiển thị
            edited_cp = st.data_editor(
                df_cp, num_rows="dynamic", use_container_width=True,
                column_config={
                    "Ngày": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "Tiền": st.column_config.NumberColumn(format="%d"),
                    "Mã căn": st.column_config.TextColumn(),
                    "Chỉ số đồng hồ": st.column_config.TextColumn(width="medium", help="Ghi lại số điện/nước cũ và mới")
                }
            )
            if st.button("💾 LƯU LÊN ĐÁM MÂY (CHI PHÍ)", type="primary"):
                save_data(edited_cp, "CHI_PHI"); time.sleep(1); st.rerun()

        # --- TAB 4: TỔNG HỢP DỮ LIỆU ---
        with tabs[3]:
            st.subheader("📋 Dữ Liệu Hợp Đồng (Online)")
            if df_main.empty: 
                st.warning("⚠️ Hiện chưa có dữ liệu nào.")
                df_show = pd.DataFrame(columns=COLUMNS)
            else:
                # --- TÍNH NĂNG MỚI: TÌM KIẾM ---
                search_term = st.text_input("🔍 Tìm kiếm nhanh (Nhập tên khách, số phòng...):")
                df_show = df_main
                
                if search_term:
                    mask = df_show.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                    df_show = df_show[mask]
                    st.success(f"🔎 Tìm thấy {len(df_show)} kết quả.")

            edited_df = st.data_editor(
                df_show, num_rows="dynamic", use_container_width=True,
                column_config={
                    "Ngày ký": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "Ngày hết HĐ": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "Ngày in": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "Ngày out": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "Giá": st.column_config.NumberColumn(format="%d"),
                    "Mã căn": st.column_config.TextColumn(),
                }
            )
            if st.button("💾 LƯU LÊN ĐÁM MÂY (HỢP ĐỒNG)", type="primary"):
                save_data(edited_df, "HOP_DONG"); time.sleep(1); st.rerun()

        # --- TAB 5: CẢNH BÁO PHÒNG ---
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
                
                if df_warning_hd.empty:
                    st.success("✅ Không có phòng nào sắp hết hạn Hợp Đồng.")
                else:
                    for idx, row in df_warning_hd.iterrows():
                        days = (row['Ngày hết HĐ'] - today).days
                        status = "ĐÃ QUÁ HẠN" if days < 0 else f"Còn {days} ngày"
                        color = "red" if days < 0 else "orange"
                        
                        with st.expander(f"🔴 {row['Mã căn']} - {row['Tên khách thuê']} ({status})"):
                            st.write(f"📅 Ngày hết HĐ: {row['Ngày hết HĐ'].strftime('%d/%m/%Y')}")
                            st.write("👉 **Mẫu tin nhắn Zalo Gia Hạn:**")
                            zalo_msg_hd = f"""Chào bạn {row['Tên khách thuê']},
BQL Tòa nhà {row['Tòa nhà']} xin thông báo:
Hợp đồng phòng {row['Mã căn']} của bạn sắp hết hạn vào ngày {row['Ngày hết HĐ'].strftime('%d/%m/%Y')}.
Vui lòng liên hệ lại với chúng tôi để gia hạn Hợp đồng nhé.
Cảm ơn bạn!"""
                            st.code(zalo_msg_hd, language=None)

                st.divider()

                st.write("#### 2️⃣ Cảnh báo Khách Sắp Trả Phòng (Check-out)")
                
                def check_out(row):
                    x = row['Ngày out']
                    if pd.isna(x): return "N/A"
                    days = (x - today).days
                    if 0 <= days <= 7: return "Sắp out"
                    return "Còn ở"
                
                df_warning_out = df_alert[df_alert.apply(lambda r: check_out(r) == "Sắp out", axis=1)]
                
                if df_warning_out.empty:
                    st.success("✅ Không có phòng nào sắp trả phòng trong 7 ngày tới.")
                else:
                    st.warning(f"🚪 Có {len(df_warning_out)} phòng sắp trả phòng!")
                    for idx, row in df_warning_out.iterrows():
                        days = (row['Ngày out'] - today).days
                        with st.expander(f"🚪 {row['Mã căn']} - {row['Tên khách thuê']} (Còn {days} ngày)"):
                            st.write(f"📅 Ngày trả phòng dự kiến: {row['Ngày out'].strftime('%d/%m/%Y')}")
                            st.write("👉 **Mẫu tin nhắn Zalo Trả Phòng:**")
                            zalo_msg_out = f"""Chào bạn {row['Tên khách thuê']},
Phòng {row['Mã căn']} của bạn sẽ đến ngày trả phòng vào {row['Ngày out'].strftime('%d/%m/%Y')}.
Bạn vui lòng vệ sinh phòng sạch sẽ và liên hệ BQL để làm thủ tục bàn giao, chốt điện nước nhé.
Cảm ơn bạn đã ở tại {row['Tòa nhà']}!"""
                            st.code(zalo_msg_out, language=None)


        # --- TAB 6: QUẢN LÝ CHI PHÍ (GHI CHÚ TỰ ĐỘNG NGÀY THÁNG) ---
        with tabs[5]:
            st.subheader("💰 Quản Lý Chi Phí & Doanh Thu Chi Tiết")
            if not df_main.empty:
                cols_to_show = [
                    "Toà", "Mã căn",
                    "Giá HĐ", "TT cho chủ nhà", "Cọc cho chủ nhà",
                    "Giá", "KH thanh toán", "KH cọc",
                    "SALE THẢO", "SALE NGA", "SALE LINH",
                    "Công ty", "Cá Nhân"
                ]
                
                # --- THÊM TẠM THỜI CÁC CỘT NGÀY VÀO ĐỂ TÍNH TOÁN GHI CHÚ ---
                cols_with_dates = cols_to_show + ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]
                existing_cols = [c for c in cols_with_dates if c in df_main.columns]
                
                df_view = df_main[existing_cols].copy()

                df_view = df_view.rename(columns={
                    "TT cho chủ nhà": "Thanh toán HĐ",
                    "Cọc cho chủ nhà": "Cọc HĐ",
                    "Giá": "Giá thuê",
                    "KH thanh toán": "Khách thanh toán",
                    "KH cọc": "Khách cọc",
                    "Công ty": "HH Công ty",
                    "Cá Nhân": "HH Cá nhân"
                })

                if "Mã căn" in df_view.columns:
                     df_view = df_view.sort_values(by=["Toà", "Mã căn"])
                else:
                     df_view = df_view.sort_values(by=["Toà"])

                # --- TẠO GHI CHÚ TỰ ĐỘNG TỪ NGÀY THÁNG ---
                def make_note(row):
                    def d(x): 
                        if pd.isna(x): return "?"
                        try: return x.strftime('%d/%m/%y')
                        except: return str(x)
                    
                    k = d(row.get('Ngày ký'))
                    h = d(row.get('Ngày hết HĐ'))
                    i = d(row.get('Ngày in'))
                    o = d(row.get('Ngày out'))
                    
                    note_parts = []
                    if k != "?" or h != "?": note_parts.append(f"HĐ: {k}-{h}")
                    if i != "?" or o != "?": note_parts.append(f"Khách: {i}-{o}")
                    return " | ".join(note_parts)

                df_view["Ghi chú"] = df_view.apply(make_note, axis=1)
                df_view = df_view.drop(columns=["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"], errors='ignore')

                numeric_cols = [
                    "Giá HĐ", "Thanh toán HĐ", "Cọc HĐ", 
                    "Giá thuê", "Khách thanh toán", "Khách cọc", 
                    "SALE THẢO", "SALE NGA", "SALE LINH", 
                    "HH Công ty", "HH Cá nhân"
                ]
                
                total_row = pd.DataFrame(df_view[numeric_cols].sum(numeric_only=True)).T
                total_row["Toà"] = "TỔNG CỘNG"
                total_row = total_row.fillna("")
                
                df_final_view = pd.concat([df_view, total_row], ignore_index=True)

                for col in numeric_cols:
                    if col in df_final_view.columns:
                        df_final_view[col] = df_final_view[col].apply(fmt_vnd)

                st.dataframe(
                    df_final_view, 
                    use_container_width=True,
                    column_config={
                        "Ghi chú": st.column_config.TextColumn(width="medium", help="Thông tin ngày tháng hợp đồng")
                    }
                )
            else:
                st.info("Chưa có dữ liệu để tổng hợp.")

        # --- TAB 7: TỔNG HỢP CHI PHÍ (MỚI - ĐÃ CẬP NHẬT GHI CHÚ VÀ GIẢI THÍCH ÂM) ---
        with tabs[6]:
            st.subheader("📊 Tổng Hợp Chi Phí & Lợi Nhuận")
            
            if not df_main.empty:
                # 1. Chuẩn bị dữ liệu tính toán
                df_calc = df_main.copy()
                
                # Hàm tính số tháng (tương đối: lấy số ngày / 30)
                def get_months(start, end):
                    if pd.isna(start) or pd.isna(end): return 0
                    try:
                        days = (end - start).days
                        return max(0, days / 30)
                    except: return 0
                    
                # 2. Tính toán các cột phức tạp
                # -- Chi phí hợp đồng: (Giá HĐ * Tháng)
                df_calc['Tháng HĐ'] = df_calc.apply(lambda r: get_months(r['Ngày ký'], r['Ngày hết HĐ']), axis=1)
                df_calc['Tổng giá trị HĐ'] = (df_calc['Giá HĐ'] * df_calc['Tháng HĐ']) # Gross Contract Value
                
                # -- Chi phí vốn theo khách (COGS): (Giá HĐ * Tháng Khách)
                df_calc['Tháng Thuê'] = df_calc.apply(lambda r: get_months(r['Ngày in'], r['Ngày out']), axis=1)
                df_calc['Chi phí vốn (theo khách)'] = (df_calc['Giá HĐ'] * df_calc['Tháng Thuê'])
                
                # -- Tổng chi phí Sale
                df_calc['Tổng Chi Phí Sale'] = df_calc['SALE THẢO'] + df_calc['SALE NGA'] + df_calc['SALE LINH']
                
                # -- Doanh thu Gộp (Gross Revenue from Guest)
                df_calc['Doanh thu cho thuê'] = (df_calc['Giá'] * df_calc['Tháng Thuê'])

                # -- Lợi Nhuận Ròng (Net Profit) = Rev - COGS - Sales - HH
                df_calc['Lợi nhuận ròng'] = df_calc['Doanh thu cho thuê'] - df_calc['Chi phí vốn (theo khách)'] - df_calc['Tổng Chi Phí Sale'] - df_calc['Công ty'] - df_calc['Cá Nhân']

                # 3. Tạo cột Ghi chú với CẢNH BÁO THÔNG MINH
                def make_smart_note(row, profit, cogs):
                    def d(x): 
                        if pd.isna(x): return "?"
                        try: return x.strftime('%d/%m/%y')
                        except: return str(x)
                    
                    k = d(row.get('Ngày ký')); h = d(row.get('Ngày hết HĐ'))
                    i = d(row.get('Ngày in')); o = d(row.get('Ngày out'))
                    
                    base_note = []
                    if k != "?" or h != "?": base_note.append(f"HĐ: {k}-{h}")
                    if i != "?" or o != "?": base_note.append(f"Khách: {i}-{o}")
                    
                    # --- PHẦN GIẢI THÍCH SỐ ÂM ---
                    warnings = []
                    if cogs == 0 and profit == 0: warnings.append("⚠️ Chưa có ngày tháng")
                    elif profit < 0: 
                        if row.get('Tháng Thuê', 0) == 0: warnings.append("⚠️ Lỗi ngày khách")
                        else: warnings.append("📉 Lỗ vốn (Thu < Chi)")
                    
                    full_note = " | ".join(base_note)
                    if warnings:
                        full_note += " || " + " ".join(warnings)
                    
                    return full_note

                df_calc["Ghi chú"] = df_calc.apply(
                    lambda r: make_smart_note(r, r['Lợi nhuận ròng'], r['Chi phí vốn (theo khách)']), 
                    axis=1
                )

                # 4. Chọn và sắp xếp cột (Đưa Ghi chú xuống cuối)
                cols_final = [
                    "Toà", "Mã căn", 
                    "Tổng giá trị HĐ", "Chi phí vốn (theo khách)", 
                    "Doanh thu cho thuê", # <-- CỘT MỚI
                    "Tổng Chi Phí Sale", "Công ty", "Cá Nhân", 
                    "Lợi nhuận ròng", "Ghi chú" 
                ]
                
                if "Mã căn" in df_calc.columns:
                     df_calc = df_calc.sort_values(by=["Toà", "Mã căn"])
                
                df_show_final = df_calc[cols_final].copy()
                
                # 5. Dòng tổng cộng
                total_row = pd.DataFrame(df_show_final.sum(numeric_only=True)).T
                total_row["Toà"] = "TỔNG CỘNG"
                total_row = total_row.fillna("")
                
                df_result = pd.concat([df_show_final, total_row], ignore_index=True)
                
                # 6. Tô màu (Highlight) cho Lợi Nhuận
                def highlight_profit(val):
                    if isinstance(val, (int, float)):
                        if val < 0: return 'color: red; font-weight: bold'
                        if val > 0: return 'color: green; font-weight: bold'
                    return ''

                # 7. Định dạng số tiền
                num_cols = ["Tổng giá trị HĐ", "Chi phí vốn (theo khách)", "Doanh thu cho thuê", "Tổng Chi Phí Sale", "Công ty", "Cá Nhân", "Lợi nhuận ròng"]
                
                # Copy ra bản để hiển thị
                df_display = df_result.copy()
                
                # Hiển thị
                st.dataframe(
                    df_display.style.applymap(highlight_profit, subset=["Lợi nhuận ròng"]).format(
                        "{:,.0f}", subset=pd.IndexSlice[0:len(df_display)-1, num_cols]
                    ),
                    use_container_width=True,
                    column_config={
                        "Ghi chú": st.column_config.TextColumn(width="large"),
                        "Tổng giá trị HĐ": st.column_config.NumberColumn(help="Giá HĐ x Tổng tháng HĐ"),
                        "Chi phí vốn (theo khách)": st.column_config.NumberColumn(help="Giá HĐ x Tháng khách ở"),
                        "Doanh thu cho thuê": st.column_config.NumberColumn(help="Giá Thuê x Tháng khách ở"),
                        "Lợi nhuận ròng": st.column_config.NumberColumn(help="Doanh thu khách - Giá vốn - Chi phí")
                    }
                )
            else:
                st.info("Chưa có dữ liệu.")

        # --- TAB 8: QUẢN LÝ DÒNG TIỀN (CHI TIẾT + GHI CHÚ) ---
        with tabs[7]:
            st.subheader("💸 Quản Lý Dòng Tiền (Thực Thu - Thực Chi)")
            
            if not df_main.empty:
                df_cf = df_main.copy()
                
                # 1. Tính toán chi tiết THỰC THU
                df_cf['Thu: Thanh toán'] = df_cf['KH thanh toán']
                df_cf['Thu: Cọc'] = df_cf['KH cọc']
                df_cf['TỔNG THU'] = df_cf['Thu: Thanh toán'] + df_cf['Thu: Cọc']
                
                # 2. Tính toán chi tiết THỰC CHI
                df_cf['Chi: Chủ nhà'] = df_cf['TT cho chủ nhà'] + df_cf['Cọc cho chủ nhà']
                df_cf['Chi: Hoa hồng'] = df_cf['SALE THẢO'] + df_cf['SALE NGA'] + df_cf['SALE LINH'] + \
                                         df_cf['Công ty'] + df_cf['Cá Nhân']
                
                # Merge Chi phí vận hành (từ tab Chi Phí Nội Bộ)
                df_op_cost = pd.DataFrame()
                if not df_cp.empty:
                    df_op_cost = df_cp.groupby("Mã căn")["Tiền"].sum().reset_index()
                    df_op_cost.columns = ["Mã căn", "Chi: Vận hành"]
                
                df_final_cf = pd.merge(df_cf, df_op_cost, on="Mã căn", how="left").fillna(0)
                
                df_final_cf['TỔNG CHI'] = df_final_cf['Chi: Chủ nhà'] + df_final_cf['Chi: Hoa hồng'] + df_final_cf['Chi: Vận hành']
                
                # 3. Dòng tiền ròng
                df_final_cf['DÒNG TIỀN RÒNG'] = df_final_cf['TỔNG THU'] - df_final_cf['TỔNG CHI']
                
                # 4. Tạo Ghi chú giải thích Dòng tiền ÂM
                def explain_cf(row):
                    net = row['DÒNG TIỀN RÒNG']
                    if net >= 0: return "✅ Dương"
                    
                    reasons = []
                    # Logic suy luận
                    if row['TỔNG THU'] == 0: reasons.append("⚠️ Chưa thu tiền khách")
                    elif row['Chi: Chủ nhà'] > 0 and row['TỔNG THU'] < row['Chi: Chủ nhà']: reasons.append("⚠️ Đã trả chủ nhà > Thu khách")
                    
                    if row['Chi: Vận hành'] > 500000: # Ngưỡng ví dụ
                        reasons.append(f"⚠️ Chi vận hành cao ({fmt_vnd(row['Chi: Vận hành'])})")
                        
                    return "; ".join(reasons)

                df_final_cf['Ghi chú'] = df_final_cf.apply(explain_cf, axis=1)

                # 5. Hiển thị
                cols_cf_show = [
                    "Toà", "Mã căn", 
                    "Thu: Thanh toán", "Thu: Cọc", "TỔNG THU",
                    "Chi: Chủ nhà", "Chi: Hoa hồng", "Chi: Vận hành", "TỔNG CHI",
                    "DÒNG TIỀN RÒNG", "Ghi chú"
                ]
                
                if "Mã căn" in df_final_cf.columns: df_final_cf = df_final_cf.sort_values(by=["Toà", "Mã căn"])
                
                df_cf_display = df_final_cf[cols_cf_show].copy()
                
                total_row_cf = pd.DataFrame(df_cf_display.sum(numeric_only=True)).T
                total_row_cf["Toà"] = "TỔNG CỘNG"; total_row_cf = total_row_cf.fillna("")
                df_cf_result = pd.concat([df_cf_display, total_row_cf], ignore_index=True)
                
                def highlight_cf(val):
                    if isinstance(val, (int, float)):
                        if val < 0: return 'color: red; font-weight: bold'
                        if val > 0: return 'color: green; font-weight: bold'
                    return ''
                
                num_cols_cf = ["Thu: Thanh toán", "Thu: Cọc", "TỔNG THU", "Chi: Chủ nhà", "Chi: Hoa hồng", "Chi: Vận hành", "TỔNG CHI", "DÒNG TIỀN RÒNG"]
                
                st.dataframe(
                    df_cf_result.style.applymap(highlight_cf, subset=["DÒNG TIỀN RÒNG"]).format(
                        "{:,.0f}", subset=pd.IndexSlice[0:len(df_cf_result)-1, num_cols_cf]
                    ),
                    use_container_width=True,
                    column_config={
                        "Ghi chú": st.column_config.TextColumn(width="large")
                    }
                )

            else:
                st.info("Chưa có dữ liệu.")

else:
    st.warning("👈 Vui lòng tải file **JSON Chìa Khóa** từ Google lên đây.")