import streamlit as st
import pandas as pd
from datetime import date

st.set_page_config(page_title="Quản lý Khách sạn Chuyên sâu", layout="wide")
st.title("🏨 Hệ Thống Quản Lý Kinh Doanh Căn Hộ/Khách Sạn")

# Tạo các Tab quản lý
tab1, tab2, tab3, tab4 = st.tabs(["🏠 Quản lý Phòng", "📝 Hợp đồng Gốc", "🔑 Khách Thuê (Đầu ra)", "💰 Chi phí & Lợi nhuận"])

# --- TAB 1: QUẢN LÝ PHÒNG ---
with tab1:
    st.header("Thông tin danh mục phòng")
    col1, col2 = st.columns(2)
    with col1:
        ma_toa = st.text_input("Mã tòa nhà")
        ma_can = st.text_input("Mã căn hộ/phòng")
    with col2:
        khu_vuc = st.text_input("Thuộc khu vực")
        chu_nha = st.text_input("Tên chủ nhà")

# --- TAB 2: QUẢN LÝ ĐẦU VÀO (HỢP ĐỒNG GỐC) ---
with tab2:
    st.header("Chi tiết hợp đồng thuê gốc")
    c1, c2, c3 = st.columns(3)
    ngay_ky = c1.date_input("Ngày ký HĐ", date.today())
    ngay_het = c2.date_input("Ngày hết HĐ", date.today())
    gia_goc = c3.number_input("Giá thuê từ chủ nhà (VNĐ/tháng)", min_value=0, step=500000)

# --- TAB 3: QUẢN LÝ ĐẦU RA (KHÁCH THUÊ) ---
with tab3:
    st.header("Thông tin khách đang thuê")
    cx, cy, cz = st.columns(3)
    ten_khach = cx.text_input("Tên khách hàng")
    ngay_in = cy.date_input("Ngày Check-in")
    ngay_out = cz.date_input("Ngày Check-out")
    gia_cho_thue = st.number_input("Giá cho khách thuê (VNĐ)", min_value=0, step=500000)

# --- TAB 4: CHI PHÍ & LỢI NHUẬN ---
with tab4:
    st.header("Quản lý Chi phí & Tính toán Lợi nhuận")
    
    st.subheader("1. Chi phí hoa hồng")
    col_a, col_b = st.columns(2)
    tien_sale = col_a.number_input("Tiền hoa hồng (VNĐ)", min_value=0)
    nguoi_huong = col_b.text_input("Người hưởng hoa hồng (Sale)")
    
    st.subheader("2. Chi phí nội bộ")
    ca, cb, cc, cd = st.columns(4)
    dien = ca.number_input("Tiền điện", min_value=0)
    nuoc = cb.number_input("Tiền nước", min_value=0)
    internet = cc.number_input("Internet", min_value=0)
    khac = cd.number_input("Chi phí khác", min_value=0)
    
    # TÍNH TOÁN LỢI NHUẬN
    st.divider()
    tong_chi_phi = gia_goc + tien_sale + dien + nuoc + internet + khac
    loi_nhuan = gia_cho_thue - tong_chi_phi
    
    c_doanhthu, c_chiphi, c_loinhuan = st.columns(3)
    c_doanhthu.metric("DOANH THU ĐẦU RA", f"{gia_cho_thue:,} đ")
    c_chiphi.metric("TỔNG CHI PHÍ ĐẦU VÀO", f"{tong_chi_phi:,} đ", delta_color="inverse")
    
    # Hiển thị màu sắc cho lợi nhuận
    if loi_nhuan > 0:
        c_loinhuan.success(f"LỢI NHUẬN: {loi_nhuan:,} đ")
    else:
        c_loinhuan.error(f"LỖ/HÒA VỐN: {loi_nhuan:,} đ")

if st.button("Xác nhận và Xuất báo cáo"):
    st.balloons()
    st.info(f"Hệ thống đã sẵn sàng lưu dữ liệu cho căn {ma_can} thuộc tòa {ma_toa}")