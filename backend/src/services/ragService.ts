import axios from 'axios';

const RAG_API_URL = "https://nyi8rynq8l.execute-api.ap-southeast-1.amazonaws.com/default/ragsearch";

export interface LegalChunk {
    title: string;
    text: string;
    score: number;
    doc_id: string;
}

export const searchLegalDocs = async (query: string, topK: number = 5): Promise<string> => {
    try {
        console.log(`[RAG] Searching for: ${query}`);
        const response = await axios.post(RAG_API_URL, {
            query: query,
            top_k: topK,
            filters: {
                source_type: ["legal"] 
            }
        });

        const results: LegalChunk[] = response.data.results || [];

        if (results.length === 0) return "";
        let contextString = "Dưới đây là các văn bản pháp luật liên quan:\n";
        results.forEach((item, index) => {
            contextString += `\n[Tài liệu ${index + 1}]: ${item.title} (${item.doc_id})\n`;
            contextString += `Nội dung: ${item.text}\n`;
            contextString += "-----------------------------------\n";
        });

        return contextString;

    } catch (error) {
        console.error("[RAG] Error calling search API:", error);
        return ""; 
    }
};