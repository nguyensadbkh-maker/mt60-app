import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, datetime
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

# --- 2. GIAO DIỆN CHÍNH ---
st.title("☁️ MT60 STUDIO - ONLINE")

# --- 3. KHU VỰC ĐĂNG NHẬP (UPLOAD CHÌA KHÓA) ---
st.sidebar.header("🔐 Đăng Nhập Hệ Thống")
st.sidebar.info("Để bảo mật tuyệt đối, vui lòng tải file Chìa khóa (JSON) của bạn lên đây để mở khóa dữ liệu.")

# Nút upload file chìa khóa
uploaded_key = st.sidebar.file_uploader("Chọn file JSON từ máy tính", type=['json'])

# --- 4. HÀM KẾT NỐI (CHỈ CHẠY KHI CÓ FILE) ---
@st.cache_resource
def connect_google_sheet(json_file):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        # Đọc trực tiếp từ file bạn vừa upload (Không sợ lỗi định dạng nữa)
        json_file.seek(0) # Đưa con trỏ về đầu file
        creds_dict = json.load(json_file)
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sh = client.open(SHEET_NAME)
        return sh
    except Exception as e:
        st.error(f"❌ File chìa khóa không đúng hoặc lỗi kết nối: {e}")
        return None

# --- 5. LOGIC CHƯƠNG TRÌNH ---
if uploaded_key is not None:
    # Nếu đã upload file -> Tiến hành kết nối
    with st.spinner("Đang mở khóa dữ liệu..."):
        sh = connect_google_sheet(uploaded_key)
    
    if sh:
        st.sidebar.success("✅ Đã kết nối thành công!")
        
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

        # --- TIỆN ÍCH ---
        def to_num(val):
            if isinstance(val, str): 
                val = val.replace(',', '').replace('.', '').strip()
                if val == '' or val.lower() == 'nan': return 0
            try: return float(val)
            except: return 0

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

        def ai_write_marketing(api_key, features, tone):
            if not AI_AVAILABLE: return "Lỗi thư viện AI"
            try:
                client = genai.Client(api_key=api_key)
                res = client.models.generate_content(model="gemini-1.5-flash", contents=f"Viết bài Facebook thuê phòng Studio. Đặc điểm: {features}. Giọng: {tone}. Có Emoji.")
                return res.text
            except Exception as e: return f"Lỗi AI: {str(e)}"

        # --- LOAD DỮ LIỆU ---
        df_main = load_data("HOP_DONG")
        df_cp = load_data("CHI_PHI")

        if not df_main.empty:
            if "Mã căn" in df_main.columns: df_main["Mã căn"] = df_main["Mã căn"].astype(str)
            for c in ["Ngày ký", "Ngày hết HĐ", "Ngày in", "Ngày out"]:
                if c in df_main.columns: df_main[c] = pd.to_datetime(df_main[c], errors='coerce')
            for c in ["Giá", "Giá HĐ", "SALE THẢO", "SALE NGA", "SALE LINH", "Công ty", "Cá Nhân"]:
                if c in df_main.columns: df_main[c] = df_main[c].apply(to_num)

        if not df_cp.empty:
            if "Ngày" in df_cp.columns: df_cp["Ngày"] = pd.to_datetime(df_cp["Ngày"], errors='coerce')
            if "Mã căn" in df_cp.columns: df_cp["Mã căn"] = df_cp["Mã căn"].astype(str)
            if "Tiền" in df_cp.columns: df_cp["Tiền"] = df_cp["Tiền"].apply(to_num)

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
                             st.caption(f"{r['Mã căn']}: {msg}")
                    if not df_kh.empty:
                        st.warning(f"🟡 {len(df_kh)} Khách sắp out")
                        for _, r in df_kh.iterrows(): st.caption(f"{r['Mã căn']}: {(r['Ngày out']-today).days} ngày")
            
            if st.button("🔄 Tải lại dữ liệu (F5)"): 
                st.cache_data.clear()
                st.rerun()

        # --- CÁC TAB CHỨC NĂNG ---
        tabs = st.tabs([
            "📊 Dữ Liệu", "📈 Biểu Đồ", "✍️ Marketing", "🔮 Nhập Liệu", 
            "🏠 Cảnh Báo", "💸 Chi Phí", "💰 Doanh Thu", "📅 Lịch", "📥 Excel"
        ])

        # --- TAB 1: DỮ LIỆU ---
        with tabs[0]:
            st.subheader("📋 Dữ Liệu Hợp Đồng (Online)")
            if df_main.empty: 
                st.warning("⚠️ Hiện chưa có dữ liệu nào.")
                df_show = pd.DataFrame(columns=COLUMNS)
            else:
                st.write(f"✅ Đang hiển thị {len(df_main)} dòng dữ liệu.")
                df_show = df_main

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

        # --- TAB 2: BIỂU ĐỒ ---
        with tabs[1]:
            if not df_main.empty:
                c1, c2 = st.columns(2)
                with c1: st.bar_chart(df_main.groupby("Toà")["Giá"].sum())
                with c2: st.bar_chart(df_main[["SALE THẢO", "SALE NGA", "SALE LINH"]].sum(), color="#FF4B4B")
                st.divider()
                df_sum = df_main.groupby("Toà")[["Giá", "Giá HĐ", "SALE THẢO", "SALE NGA", "SALE LINH"]].sum().reset_index()
                st.dataframe(df_sum, use_container_width=True)

        # --- TAB 3: MARKETING ---
        with tabs[2]:
            c1, c2 = st.columns([1, 2])
            with c1:
                key_mkt = st.text_input("Google API Key", type="password", key="key_mkt")
                feat = st.text_area("Đặc điểm", height=100); tone = st.selectbox("Giọng văn", ["Sôi động", "Sang trọng", "Thân thiện"])
                if st.button("Viết bài"): st.session_state['content_mkt'] = ai_write_marketing(key_mkt, feat, tone)
            with c2:
                if 'content_mkt' in st.session_state: st.text_area("Kết quả:", value=st.session_state['content_mkt'], height=400)

        # --- TAB 4: NHẬP LIỆU ---
        with tabs[3]:
            st.subheader("🔮 Nhập Liệu Thông Minh")
            c_txt, c_img = st.columns(2)
            with c_txt:
                txt = st.text_area("Tin nhắn Zalo:"); 
                if st.button("Phân tích Text"): st.session_state['auto'] = parse_text_message(txt)
            with c_img:
                key_vis = st.text_input("API Key (Vision)", type="password", key="key_vis")
                up = st.file_uploader("Upload ảnh", type=["jpg", "png"])
                if up and key_vis and st.button("Phân tích Ảnh"):
                    with st.spinner("AI đang đọc..."): st.session_state['auto'] = parse_image_gemini(key_vis, Image.open(up))

            st.divider()
            av = st.session_state.get('auto', {})
            with st.form("main_form"):
                c1, c2, c3, c4 = st.columns(4)
                d = {}
                with c1:
                    d["Tòa nhà"] = st.text_input("Tòa nhà", value=str(av.get("toa_nha","")))
                    d["Mã căn"] = st.text_input("Mã căn", value=str(av.get("ma_can","")))
                    d["Toà"] = st.text_input("Toà")
                    d["Chủ nhà - sale"] = st.text_input("Chủ nhà - sale")
                with c2:
                    d["Ngày ký"] = st.date_input("Ngày ký", date.today())
                    d["Ngày hết HĐ"] = st.date_input("Ngày hết HĐ", date.today())
                    d["Giá HĐ"] = st.number_input("Giá HĐ", min_value=0)
                    d["TT cho chủ nhà"] = st.text_input("TT cho chủ nhà")
                with c3:
                    d["Tên khách thuê"] = st.text_input("Tên khách", value=str(av.get("ten_khach","")))
                    def safe_d(v): 
                        try: return pd.to_datetime(v).date() 
                        except: return date.today()
                    d["Ngày in"] = st.date_input("Ngày in", safe_d(av.get("ngay_in")))
                    d["Ngày out"] = st.date_input("Ngày out", safe_d(av.get("ngay_out")))
                    d["Giá"] = st.number_input("Giá thuê", min_value=0, value=int(av.get("gia_thue", 0) or 0))
                with c4:
                    d["Công ty"] = st.number_input("Công ty", min_value=0)
                    d["Cá Nhân"] = st.number_input("Cá Nhân", min_value=0)
                    d["SALE THẢO"] = st.number_input("Sale Thảo", min_value=0)
                    d["SALE NGA"] = st.number_input("Sale Nga", min_value=0)
                    d["SALE LINH"] = st.number_input("Sale Linh", min_value=0)
                
                if st.form_submit_button("Lưu lên Cloud"):
                    for k, v in d.items():
                        if isinstance(v, (date, datetime)): d[k] = pd.to_datetime(v)
                    new_row = pd.DataFrame([d])
                    df_final = pd.concat([df_main, new_row], ignore_index=True)
                    save_data(df_final, "HOP_DONG"); st.session_state['auto'] = {}; time.sleep(1); st.rerun()

        # --- TAB 5: CẢNH BÁO ---
        with tabs[4]:
            st.subheader("🏠 Cảnh Báo Chi Tiết")
            if not df_main.empty:
                df_alert = df_main.sort_values('Ngày out').groupby(['Mã căn', 'Toà']).tail(1).copy()
                def check_khach(x): 
                    if pd.isna(x): return "⚪ Trống"
                    days = (x - today).days
                    if days < 0: return "⚪ Trống (Đã out)"
                    return f"🟡 Sắp out ({days} ngày)" if days <= 7 else "🟢 Đang ở"
                def check_hd(row):
                    x = row['Ngày hết HĐ']
                    if pd.isna(x): return "❓ N/A"
                    days = (x - today).days
                    if days < 0: return "🔴 ĐÃ HẾT HẠN HĐ"
                    if days <= 30: return f"⚠️ Sắp hết HĐ ({days} ngày)"
                    return "✅ Còn hạn"
                df_alert['Trạng thái Khách'] = df_alert['Ngày out'].apply(check_khach)
                df_alert['Cảnh báo HĐ'] = df_alert.apply(check_hd, axis=1)
                st.dataframe(format_date_vn(df_alert[['Mã căn', 'Toà', 'Tên khách thuê', 'Ngày out', 'Trạng thái Khách', 'Ngày hết HĐ', 'Cảnh báo HĐ']]), use_container_width=True)

        # --- TAB 6: CHI PHÍ ---
        with tabs[5]:
            st.subheader("💸 Quản Lý Chi Phí")
            with st.expander("🧮 Thêm mới & Máy tính", expanded=False):
                c1, c2, c3, c4 = st.columns(4)
                sc = c1.number_input("Số cũ", 0.0); sm = c2.number_input("Số mới", 0.0); dg = c3.number_input("Đơn giá", 3500)
                c4.metric("Thành tiền", f"{(sm-sc)*dg:,.0f}")
                with st.form("cp_form"):
                    c1, c2, c3, c4 = st.columns(4)
                    d = c1.date_input("Ngày", date.today()); can = c2.text_input("Mã căn")
                    loai = c3.selectbox("Loại", ["Điện", "Nước", "Net", "Dọn dẹp", "Khác"])
                    tien = c4.number_input("Tiền", value=float((sm-sc)*dg))
                    if st.form_submit_button("Lưu Chi Phí"):
                        new = pd.DataFrame([{"Mã căn": str(can).strip(), "Loại": loai, "Tiền": tien, "Ngày": pd.to_datetime(d)}])
                        df_cp_new = pd.concat([df_cp, new], ignore_index=True)
                        save_data(df_cp_new, "CHI_PHI"); time.sleep(1); st.rerun()
            col_up, col_down = st.columns(2)
            with col_down:
                df_mau_cp = pd.DataFrame(columns=["Ngày", "Mã căn", "Loại", "Tiền"])
                df_mau_cp.loc[0] = [date.today(), "101", "Điện", 500000]
                st.download_button("📥 Tải File Mẫu Chi Phí (.xlsx)", convert_df_to_excel(df_mau_cp), "mau_chi_phi.xlsx")
            st.divider()
            if df_cp.empty: df_cp = pd.DataFrame(columns=["Ngày", "Mã căn", "Loại", "Tiền"])
            edited_cp = st.data_editor(
                df_cp, num_rows="dynamic", use_container_width=True,
                column_config={"Ngày": st.column_config.DateColumn(format="DD/MM/YYYY"), "Tiền": st.column_config.NumberColumn(format="%d"), "Mã căn": st.column_config.TextColumn()}
            )
            if st.button("💾 LƯU LÊN ĐÁM MÂY (CHI PHÍ)", type="primary"):
                save_data(edited_cp, "CHI_PHI"); time.sleep(1); st.rerun()

        # --- TAB 7: DOANH THU ---
        with tabs[6]:
            st.subheader("💰 Báo Cáo Doanh Thu & Lợi Nhuận")
            if not df_main.empty:
                cp_sum = pd.DataFrame(columns=["Mã căn", "CP Nội Bộ"])
                if not df_cp.empty:
                     cp_sum = df_cp.groupby("Mã căn")["Tiền"].sum().reset_index(); cp_sum.columns = ["Mã căn", "CP Nội Bộ"]
                final = pd.merge(df_main, cp_sum, on="Mã căn", how="left").fillna(0)
                final["Lợi Nhuận Net"] = final["Giá"] - final["Giá HĐ"] - final[["SALE THẢO", "SALE NGA", "SALE LINH"]].sum(axis=1) - final["CP Nội Bộ"] - final["Công ty"] - final["Cá Nhân"]
                grp = final.groupby("Toà")[["Giá", "Giá HĐ", "CP Nội Bộ", "Lợi Nhuận Net"]].sum().reset_index()
                total = pd.DataFrame(grp.sum(numeric_only=True)).T; total["Toà"] = "TỔNG CỘNG"
                st.dataframe(pd.concat([grp, total], ignore_index=True).style.format(precision=0, thousands="."), use_container_width=True)

        # --- TAB 8: LỊCH ---
        with tabs[7]:
            if not df_main.empty:
                df_cal = df_main.dropna(subset=['Ngày in', 'Ngày out'])
                if not df_cal.empty:
                    fig = px.timeline(df_cal, x_start="Ngày in", x_end="Ngày out", y="Mã căn", color="Toà", hover_name="Tên khách thuê")
                    fig.update_yaxes(autorange="reversed")
                    st.plotly_chart(fig, use_container_width=True)

        # --- TAB 9: EXCEL ---
        with tabs[8]:
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

else:
    # Nếu chưa upload chìa khóa thì hiện thông báo lớn
    st.warning("👈 Vui lòng tải file **credentials.json** (File Chìa khóa) lên ở cột bên trái để bắt đầu.")
    st.markdown("---")
    st.write("### ❓ Hướng dẫn:")
    st.write("1. Tìm file `.json` bạn đã tải về từ Google Cloud lúc đầu.")
    st.write("2. Kéo thả file đó vào ô **'Chọn file JSON'** ở cột bên trái.")
    st.write("3. Hệ thống sẽ tự động đăng nhập.")