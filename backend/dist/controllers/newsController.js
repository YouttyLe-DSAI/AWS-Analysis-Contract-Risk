"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getNews = void 0;
const axios_1 = __importDefault(require("axios"));
const cheerio = __importStar(require("cheerio"));
// --- 1. Các hàm Helper (Lấy ảnh, Parse tin) ---
const BACKUP_IMAGES = {
    law: "https://images.unsplash.com/photo-1589829085413-56de8ae18c73?auto=format&fit=crop&w=600&q=80",
    economy: "https://images.unsplash.com/photo-1460925895917-afdab827c52f?auto=format&fit=crop&w=600&q=80",
    tech: "https://images.unsplash.com/photo-1518770660439-4636190af475?auto=format&fit=crop&w=600&q=80",
    meeting: "https://images.unsplash.com/photo-1556761175-5973dc0f32e7?auto=format&fit=crop&w=600&q=80",
    default: "https://images.unsplash.com/photo-1450101499163-c8848c66ca85?auto=format&fit=crop&w=600&q=80"
};
const getSmartImage = (title) => {
    const t = title.toLowerCase();
    if (t.includes("kinh tế") || t.includes("ngân hàng"))
        return BACKUP_IMAGES.economy;
    if (t.includes("số") || t.includes("công nghệ"))
        return BACKUP_IMAGES.tech;
    if (t.includes("hội nghị") || t.includes("chỉ đạo"))
        return BACKUP_IMAGES.meeting;
    if (t.includes("luật") || t.includes("nghị định"))
        return BACKUP_IMAGES.law;
    return BACKUP_IMAGES.default;
};
function fetchArticleDetails(url) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const { data } = yield axios_1.default.get(url, { headers: { "User-Agent": "Mozilla/5.0" }, timeout: 4000 });
            const $ = cheerio.load(data);
            const imgTag = $(".detail-content figure img").first();
            let image = imgTag.attr("data-original") || imgTag.attr("src") || "";
            const time = $(".detail-time").text().trim() || $(".article-header .meta").text().trim();
            if (image && !image.startsWith("http") && url.includes("baochinhphu"))
                image = "https://baochinhphu.vn" + image;
            return { image, time };
        }
        catch (e) {
            return { image: "", time: "" };
        }
    });
}
function parseNews($, sourceName, tagName) {
    const items = [];
    $(".box-stream-item, .av-item, .story").each((i, el) => {
        if (i > 5)
            return;
        const titleEl = $(el).find(".box-stream-link-title, h3 a, h2 a, .story__heading a").first();
        const title = titleEl.text().trim();
        let link = titleEl.attr("href");
        const desc = $(el).find(".box-stream-sapo, .summary, .story__summary").text().trim();
        const time = $(el).find(".box-stream-meta, .time, .story__meta").text().trim();
        const imgTag = $(el).find("img").first();
        let img = imgTag.attr("data-src") || imgTag.attr("data-original") || imgTag.attr("src") || "";
        if (link && !link.startsWith("http")) {
            link = sourceName === "Báo Chính Phủ" ? "https://baochinhphu.vn" + link : "https://chinhphu.vn" + link;
        }
        if (img && !img.startsWith("http") && !img.startsWith("data:")) {
            img = sourceName === "Báo Chính Phủ" ? "https://baochinhphu.vn" + img : "https://chinhphu.vn" + img;
        }
        if (!img || img.includes("base64") || img.includes("icon"))
            img = getSmartImage(title);
        if (title && link) {
            items.push({
                id: `news-${tagName}-${i}`,
                title, link, desc: desc || "Tin tức mới cập nhật.",
                source: sourceName, tag: tagName,
                date: time || "Vừa xong", image: img
            });
        }
    });
    return items;
}
// --- 2. Controller Chính ---
const getNews = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const pinnedLinks = [
            {
                link: "https://baochinhphu.vn/day-nhanh-tien-do-cac-du-an-truyen-tai-dien-tren-dia-ban-tinh-ca-mau-10225111210171423.htm",
                tag: "KINH TẾ", id: "pin-camau",
                title: "Đẩy nhanh tiến độ các dự án truyền tải điện tại Cà Mau",
                desc: "UBND tỉnh Cà Mau làm việc với EVNNPT về công tác giải phóng mặt bằng các dự án điện trọng điểm."
            }
        ];
        const pinnedNews = yield Promise.all(pinnedLinks.map((item) => __awaiter(void 0, void 0, void 0, function* () {
            const details = yield fetchArticleDetails(item.link);
            return Object.assign(Object.assign({}, item), { source: "Báo Chính Phủ", date: details.time || "Hôm nay", image: details.image || getSmartImage(item.title) });
        })));
        // Gọi song song 3 nguồn tin
        const promiseKinhTe = axios_1.default.get("https://baochinhphu.vn/kinh-te.htm").then(r => parseNews(cheerio.load(r.data), "Báo Chính Phủ", "KINH TẾ")).catch(() => []);
        const promiseChinhSach = axios_1.default.get("https://baochinhphu.vn/chinh-sach-moi.htm").then(r => parseNews(cheerio.load(r.data), "Báo Chính Phủ", "CHÍNH SÁCH")).catch(() => []);
        const promiseDoanhNghiep = axios_1.default.get("https://chinhphu.vn/doanh-nghiep").then(r => parseNews(cheerio.load(r.data), "Cổng TTĐT CP", "DOANH NGHIỆP")).catch(() => []);
        const [news1, news2, news3] = yield Promise.all([promiseKinhTe, promiseChinhSach, promiseDoanhNghiep]);
        const allCrawled = [...news1, ...news2, ...news3].filter(item => !pinnedNews.some(pin => pin.link === item.link));
        res.json({ success: true, data: [...pinnedNews, ...allCrawled] });
    }
    catch (error) {
        console.error(error);
        res.status(500).json({ success: false, data: [] });
    }
});
exports.getNews = getNews;
