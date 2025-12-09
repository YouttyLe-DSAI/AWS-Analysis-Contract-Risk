// src/lib/axiosClient.ts
import axios from 'axios';
import { fetchAuthSession } from 'aws-amplify/auth';

const axiosClient = axios.create({
  baseURL: import.meta.env.VITE_API_URL, 
  headers: {
    'Content-Type': 'application/json',
  },
});

axiosClient.interceptors.request.use(async (config) => {
  try {
    const session = await fetchAuthSession();
    const token = session.tokens?.accessToken?.toString();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  } catch (error) {
    console.log("Guest user or error fetching token");
  }
  return config;
});

export default axiosClient;