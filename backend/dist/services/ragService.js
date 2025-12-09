"use strict";
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
exports.searchLegalDocs = void 0;
const axios_1 = __importDefault(require("axios"));
// URL API Gateway của Lambda Python mà bạn đã tìm thấy
const RAG_API_URL = "https://nyi8rynq8l.execute-api.ap-southeast-1.amazonaws.com/default/ragsearch";
const searchLegalDocs = (query_1, ...args_1) => __awaiter(void 0, [query_1, ...args_1], void 0, function* (query, topK = 5) {
    try {
        console.log(`[RAG] Searching for: ${query}`);
        // Gọi sang Lambda Python
        const response = yield axios_1.default.post(RAG_API_URL, {
            query: query,
            top_k: topK,
            filters: {
                source_type: ["legal"] // Chỉ tìm văn bản luật
            }
        });
        const results = response.data.results || [];
        if (results.length === 0)
            return "";
        // Format kết quả thành chuỗi văn bản để nhét vào Prompt
        let contextString = "Dưới đây là các văn bản pháp luật liên quan:\n";
        results.forEach((item, index) => {
            contextString += `\n[Tài liệu ${index + 1}]: ${item.title} (${item.doc_id})\n`;
            contextString += `Nội dung: ${item.text}\n`;
            contextString += "-----------------------------------\n";
        });
        return contextString;
    }
    catch (error) {
        console.error("[RAG] Error calling search API:", error);
        // Nếu lỗi search thì vẫn trả về rỗng để luồng chính không chết
        return "";
    }
});
exports.searchLegalDocs = searchLegalDocs;
