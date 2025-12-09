export interface ContractTemplate {
  id: string;
  name: string;
  category: string;
  description: string;
  content: string; 
}

const fakeContent = (title: string) => `
  <div style="font-family: 'Times New Roman', serif; line-height: 1.5;">
    <h2 style="text-align: center; text-transform: uppercase;">${title}</h2>
    <p style="text-align: center; font-style: italic;">(Số: .../2025/HĐ)</p>
    <br/>
    <p><b>Căn cứ:</b> Bộ luật Dân sự 2015 và các văn bản hướng dẫn thi hành...</p>
    <p>Hôm nay, ngày ... tháng ... năm ..., tại ... chúng tôi gồm:</p>
    <p><b>BÊN A:</b> [Tên Bên A]...</p>
    <p><b>BÊN B:</b> [Tên Bên B]...</p>
    <p>Hai bên thỏa thuận ký kết hợp đồng này với các điều khoản sau...</p>
  </div>
`;

export const templateLibrary: ContractTemplate[] = [
  // --- NHÓM 1: THUÊ NHÀ / VĂN PHÒNG ---
  {
    id: "template_hop_dong_thue_hop_đong_thue_can_ho_chung_cu",
    name: "Hợp đồng Thuê Căn hộ Chung cư",
    category: "Bất động sản",
    description: "Dành cho thuê căn hộ chung cư để ở hoặc làm văn phòng.",
    content: fakeContent("HỢP ĐỒNG THUÊ CĂN HỘ CHUNG CƯ")
  },
  {
    id: "template_hop_dong_thue_hop_đong_thue_nha_tro",
    name: "Hợp đồng Thuê Nhà trọ",
    category: "Bất động sản",
    description: "Mẫu đơn giản dành cho sinh viên, người lao động thuê trọ.",
    content: fakeContent("HỢP ĐỒNG THUÊ NHÀ TRỌ")
  },
  {
    id: "template_hop_dong_thue_hop_đong_thue_nha",
    name: "Hợp đồng Thuê Nhà ở",
    category: "Bất động sản",
    description: "Mẫu hợp đồng thuê nhà nguyên căn.",
    content: fakeContent("HỢP ĐỒNG THUÊ NHÀ Ở")
  },
  {
    id: "template_hop_dong_thue_hop_đong_thue_van_phong",
    name: "Hợp đồng Thuê Văn phòng",
    category: "Kinh doanh",
    description: "Dành cho doanh nghiệp thuê mặt bằng làm văn phòng.",
    content: fakeContent("HỢP ĐỒNG THUÊ VĂN PHÒNG")
  },
  {
    id: "template_hop_dong_thue_hop_đong_cho_thue_nha_xuong_va_kho_bai",
    name: "Hợp đồng Thuê Nhà xưởng / Kho bãi",
    category: "Kinh doanh",
    description: "Dành cho thuê kho, xưởng sản xuất công nghiệp.",
    content: fakeContent("HỢP ĐỒNG THUÊ KHO BÃI")
  },

  // --- NHÓM 2: MUA BÁN / ĐẶT CỌC ---
  {
    id: "template_hop_dong_mua_ban_hop_đong_đat_coc_mua_ban_can_ho_chung_cu",
    name: "Hợp đồng Đặt cọc Mua bán Chung cư",
    category: "Bất động sản",
    description: "Thỏa thuận đặt cọc để đảm bảo giao kết hợp đồng mua bán.",
    content: fakeContent("HỢP ĐỒNG ĐẶT CỌC MUA BÁN CĂN HỘ")
  },
  {
    id: "template_hop_dong_mua_ban_hop_đong_hua_mua_hua_ban",
    name: "Hợp đồng Hứa mua - Hứa bán",
    category: "Bất động sản",
    description: "Cam kết mua bán tài sản trong tương lai.",
    content: fakeContent("HỢP ĐỒNG HỨA MUA - HỨA BÁN")
  },
  {
    id: "template_hop_dong_mua_ban_hop_đong_mua_ban_nha_o_xa_hoi",
    name: "Hợp đồng Mua bán Nhà ở Xã hội",
    category: "Bất động sản",
    description: "Dành cho giao dịch mua bán NOXH theo quy định nhà nước.",
    content: fakeContent("HỢP ĐỒNG MUA BÁN NHÀ Ở XÃ HỘI")
  },

  // --- NHÓM 3: CHUYỂN NHƯỢNG / THANH LÝ ---
  {
    id: "template_bien_ban_bien_ban_thanh_ly_hop_đong_thue_nha",
    name: "Biên bản Thanh lý Hợp đồng Thuê nhà",
    category: "Pháp lý",
    description: "Dùng khi kết thúc thời hạn thuê hoặc chấm dứt trước hạn.",
    content: fakeContent("BIÊN BẢN THANH LÝ HỢP ĐỒNG")
  },
  {
    id: "template_hop_dong_chuyen_nhuong_hop_đong_chuyen_nhuong_mot_phan_du_an_bat_đong_san",
    name: "HĐ Chuyển nhượng một phần Dự án BĐS",
    category: "Kinh doanh",
    description: "Dành cho chủ đầu tư chuyển nhượng dự án.",
    content: fakeContent("HỢP ĐỒNG CHUYỂN NHƯỢNG DỰ ÁN")
  }
];