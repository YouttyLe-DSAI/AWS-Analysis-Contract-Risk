import axiosClient from "@/lib/axiosClient";


export const uploadContractService = async (file: File) => {
  const formData = new FormData();
  formData.append('file', file);
  
  const response = await axiosClient.post('/contracts/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return response.data; 
};

export const chatContractService = async (sessionId: string, message: string) => {
  const response = await axiosClient.post('/contracts/chat', {
    sessionId,
    message
  });
  return response.data; 
};

export const generateContractService = async (templateId: string, contractInfo: any) => {
  const response = await axiosClient.post('/contracts/generate', {
    template_id: templateId,
    contract_info: contractInfo
  });
  return response.data;
};

export const getUserDashboardService = async () => {
  const response = await axiosClient.get('/contracts/dashboard');
  return response.data; 
};

export const saveUserDraftsService = async (templates: any[]) => {
  const response = await axiosClient.post('/contracts/drafts', { templates });
  return response.data;
};

export const deleteInspectionService = async (sessionId: string) => {
  const response = await axiosClient.delete(`/contracts/${sessionId}`);
  return response.data;
};

export const aiAssistService = async (prompt: string) => {
  const response = await axiosClient.post('/contracts/assist', {
    prompt: prompt
  });
  return response.data; 
};