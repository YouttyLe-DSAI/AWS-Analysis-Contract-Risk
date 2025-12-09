import React, { createContext, useContext, useEffect, useState } from "react";
import { getCurrentUser, fetchUserAttributes, signOut as amplifySignOut } from 'aws-amplify/auth';
import { Hub } from 'aws-amplify/utils';
import { getUserDashboardService, saveUserDraftsService } from '@/services/contractService';
import { updateUserProfileService, getUserProfileService, UserProfileData } from '@/services/userService';

// --- 1. KHAI BÁO CÁC INTERFACE

export interface ContractAnalysisData {
  contract: {
    title?: string;
    fullContent?: string;
    clauses: Array<{
      id: string;
      title: string;
      content: string;
      risk: "safe" | "caution" | "danger";
      suggestion: string;
    }>;
  };
  summary: {
    score: number;
    status: string;
    description: string;
    risks: Array<{ level: string; count: number }>;
  };
  overall_risk_level?: string;
}

export interface UserInspection {
  id: string;
  name: string;
  content?: string;
  score: number;
  status?: string;
  createdAt: string;
  analysisData?: ContractAnalysisData;
  fileUrl?: string;
  fileType?: string;
}

export interface UserDraft {
  id: string;
  originalTemplateId?: string;
  name: string;
  content: string;
  lastSaved: string;
  templateId?: string;
}

// --- 2. USER TYPE ---
export type User = {
  id: string;
  email?: string;
  username?: string; 
  fullName?: string; 
  avatar?: string;
  phone?: string;     
  birthdate?: string; 
  gender?: string;    
  inspections: UserInspection[];
  templates: UserDraft[];
};

type AuthContextType = {
  user: User | null;
  loading: boolean;
  signOut: () => Promise<void>;
  updateUserProfile: (data: Partial<User>) => Promise<{ error: any }>;
  refreshUser: () => Promise<void>;
};

const AuthContext = createContext<AuthContextType>({ 
    user: null, 
    loading: true, 
    signOut: async () => {},
    updateUserProfile: async () => ({ error: null }),
    refreshUser: async () => {}
});

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  const checkUser = async () => {
    try {
      const { userId } = await getCurrentUser();
      const attributes = await fetchUserAttributes();
      
      // 1. Tạo user cơ bản từ Cognito
      let finalUser: User = { 
          id: userId, 
          email: attributes.email,
          username: attributes.name || attributes.email,
          fullName: attributes.name || attributes.email,
          avatar: attributes.picture, 
          inspections: [],
          templates: []
      };

      // 2. LẤY DỮ LIỆU TỪ DB (Profile + Dashboard)
      try {
          // A. Lấy Profile chi tiết
          const profileData = await getUserProfileService();
          
          // B. Lấy Dashboard (Inspections, Drafts)
          const dashboardData = await getUserDashboardService();

          // C. Gộp dữ liệu lại
          finalUser = {
              ...finalUser,
              ...profileData, 
              username: profileData.username || finalUser.username,
              fullName: profileData.username || finalUser.fullName, 
              
              // Gộp mảng inspections/templates
              inspections: dashboardData?.inspections || [],
              templates: dashboardData?.drafts || []
          };
          
          console.log("✅ Đã sync dữ liệu User từ DB thành công");

      } catch (apiError) {
          console.warn("⚠️ Chưa có dữ liệu trong DB hoặc lỗi mạng:", apiError);
      }
      
      setUser(finalUser);
    } catch (error) {
      setUser(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    checkUser();
    const listener = Hub.listen('auth', (data) => {
      if (data.payload.event === 'signedIn') checkUser();
      if (data.payload.event === 'signedOut') setUser(null);
    });
    return () => listener();
  }, []);

  const signOut = async () => {
    try {
        await amplifySignOut();
        setUser(null);
    } catch (error) {
        console.error("Error signing out: ", error);
    }
  };

  // 3. HÀM UPDATE USER
  const updateUserProfile = async (data: Partial<User>) => {
    if (!user) return { error: "User not found" };

    const updatedUser = { ...user, ...data };
    setUser(updatedUser);

    try {
        // A. Lưu Drafts
        if (data.templates) {
            await saveUserDraftsService(data.templates);
        }

        // B. Lưu Profile cá nhân
        const { fullName, phone, birthdate, gender, avatar } = data;
        
        if (fullName || phone || birthdate || gender || avatar !== undefined) {
             console.log("💾 Đang lưu Profile xuống DB...");
             
             const payload: UserProfileData = {
                 ...data,
                 username: fullName 
             };
             
             await updateUserProfileService(payload);
        }
        
    } catch (err: any) {
        console.error("❌ Lỗi khi lưu dữ liệu xuống DB:", err);
        return { error: err };
    }

    return { error: null };
  };

  return (
    <AuthContext.Provider value={{ user, loading, signOut, updateUserProfile, refreshUser: checkUser }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => useContext(AuthContext);