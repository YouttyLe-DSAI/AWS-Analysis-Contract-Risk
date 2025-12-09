import axiosClient from '@/lib/axiosClient';

export interface UserProfileData {
  username?: string; 
  fullName?: string;
  phone?: string;
  birthdate?: string;
  gender?: string;
  avatar?: string;
}


export const getUserProfileService = async () => {
  const response = await axiosClient.get('/auth/profile');
  return response.data;
};

export const updateUserProfileService = async (data: UserProfileData) => {
  const response = await axiosClient.put('/auth/profile', data);
  return response.data;
};

export const uploadUserAvatarService = async (file: File) => {
    const formData = new FormData();
    formData.append('avatar', file); 

    const response = await axiosClient.post('/auth/avatar', formData, {
        headers: {
            'Content-Type': 'multipart/form-data',
        },
    });
    return response.data; 
};