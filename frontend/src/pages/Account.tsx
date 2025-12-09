import React, { useState, useRef, useEffect } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import { useTranslation } from '@/lib/translations';
import { User, Mail, Phone, Cake, VenetianMask, ChevronRight, X, Camera, Trash2, Edit } from 'lucide-react';
import { LanguageSwitcher } from '@/components/LanguageSwitcher';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useToast } from '@/hooks/use-toast';
import { uploadUserAvatarService } from '@/services/userService';


const InfoRow = ({ icon, label, value, onClick }: { icon: React.ElementType, label: string, value: string | React.ReactNode, onClick?: () => void }) => {
  const Icon = icon;
  return (
    <div 
      className={`flex items-center justify-between p-4 border-b last:border-b-0 ${onClick ? 'cursor-pointer hover:bg-gray-50' : ''}`}
      onClick={onClick}
    >
      <div className="flex items-center gap-4">
        <Icon className="w-6 h-6 text-gray-500" />
        <div>
          <p className="font-medium text-gray-800">{label}</p>
          <div className="text-sm text-gray-500">{value}</div>
        </div>
      </div>
      {onClick && <ChevronRight className="w-5 h-5 text-gray-400" />}
    </div>
  );
};


// --- Editing Modal --- 
const EditModal = ({ field, currentVal, onClose, onSave, isClearable }: { field: string, currentVal: string, onClose: () => void, onSave: (field: string, value: string) => void, isClearable: boolean }) => {
  const { t } = useTranslation();
  const [value, setValue] = useState(currentVal);

  const getTitle = () => {
    switch(field) {
      case 'fullName': return t.fullName;
      case 'birthdate': return t.birthdate;
      case 'gender': return t.gender;
      case 'phone': return t.phone;
      default: return 'Edit Info';
    }
  }

  const renderInput = () => {
    const sharedInputClass = "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
    if (field === 'gender') {
      return (
        <select value={value} onChange={(e) => setValue(e.target.value)} className={sharedInputClass}>
          <option value="" disabled>{t.notSet}</option>
          <option value="Male">Male</option>
          <option value="Female">Female</option>
          <option value="Other">Other</option>
        </select>
      )
    }
    return <Input type={field === 'birthdate' ? 'date' : 'text'} value={value} onChange={(e) => setValue(e.target.value)} className="pr-8" />
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex justify-center items-center z-50">
      <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-md mx-4">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-xl font-semibold">{`Edit ${getTitle()}`}</h3>
          <button onClick={onClose}><X className="h-5 w-5"/></button>
        </div>
        <div className='space-y-2'>
          <Label htmlFor={field}>{getTitle()}</Label>
          <div className="relative">
            {renderInput()}
            {isClearable && value && (
                <Button variant="ghost" size="icon" className="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7" onClick={() => setValue('')}>
                    <X className="h-4 w-4" />
                </Button>
            )}
          </div>
        </div>
        <div className="mt-6 flex justify-end gap-3">
          <Button variant="outline" onClick={onClose}>{t.cancel}</Button>
          <Button className='bg-[#4f46e5] hover:bg-[#4338ca] text-white' onClick={() => onSave(field, value)}>{t.save}</Button>
        </div>
      </div>
    </div>
  );
}

// --- Main Account Component ---
const Account = () => {
  const { user, updateUserProfile } = useAuth();
  const { t } = useTranslation();
  const { toast } = useToast();
  const [modalState, setModalState] = useState<{ isOpen: boolean; field: string; currentVal: string; isClearable: boolean; }>({ isOpen: false, field: '', currentVal: '', isClearable: false });
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [fullName, setFullName] = useState(user?.fullName || '');
  const [isEditingFullName, setIsEditingFullName] = useState(false);

  useEffect(() => {
    setFullName(user?.fullName || '');
  }, [user?.fullName]);

  const openModal = (field: string, currentVal: string) => {
    const clearableFields = ['birthdate', 'gender', 'phone', 'fullName'];
    setModalState({ 
        isOpen: true, 
        field, 
        currentVal, 
        isClearable: clearableFields.includes(field)
    });
  };

  const closeModal = () => {
    setModalState({ isOpen: false, field: '', currentVal: '', isClearable: false });
  };

  const handleSave = async (field: string, value: string) => {
    const { error } = await updateUserProfile({ [field]: value });
    if (error) {
      toast({ title: "Update failed", description: error.message, variant: "destructive" });
    } else {
      toast({ title: "Profile Updated!", description: `Your ${field} has been updated.` });
      closeModal();
      if(field === 'fullName') setIsEditingFullName(false);
    }
  };

  const handleFullNameSave = () => {
    handleSave('fullName', fullName);
  }
  
  const handleAvatarClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      try {
        if (file.size > 5 * 1024 * 1024) {
             toast({ title: "File too large", description: "Please upload an image smaller than 5MB.", variant: "destructive" });
             return;
        }

        toast({ title: "Uploading...", description: "Updating your avatar, please wait." });

        const response = await uploadUserAvatarService(file);

        if (response.success && response.avatarUrl) {
            const { error } = await updateUserProfile({ avatar: response.avatarUrl });
            
            if (!error) {
                toast({ title: "Avatar Updated!", description: "Your new avatar has been saved successfully." });
            } else {
                 console.warn("Context update warning:", error);
                 toast({ title: "Avatar Updated", description: "Image uploaded, please refresh if not visible." });
            }
        }
      } catch (error) {
        console.error("Upload error:", error);
        toast({ title: "Upload failed", description: "Could not upload image. Please try again.", variant: "destructive" });
      }
    }
  };

  const handleRemoveAvatar = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (window.confirm('Are you sure you want to remove your avatar?')) {
        const { error } = await updateUserProfile({ avatar: '' });
        if (error) {
            toast({ title: "Failed to remove avatar", description: error.message, variant: "destructive" });
        } else {
            toast({ title: "Avatar Removed", description: "Your avatar has been reset to default." });
        }
    }
  }

  return (
    <div className="bg-gray-50 min-h-screen">
      <input type="file" ref={fileInputRef} onChange={handleFileChange} accept="image/*" style={{ display: 'none' }}/>
      {modalState.isOpen && <EditModal field={modalState.field} currentVal={modalState.currentVal} onClose={closeModal} onSave={handleSave} isClearable={modalState.isClearable}/>}
      
      <div className="max-w-4xl mx-auto p-4 md:p-8">

        <header className="flex justify-between items-center mb-6">
          <h1 className="text-3xl font-bold text-gray-900">{t.accountPageTitle}</h1>
          <LanguageSwitcher />
        </header>
        <p className="text-gray-600 mb-10">{t.accountPageSubtitle}</p>

        <section className="mb-8">
          <h2 className="text-xl font-semibold text-gray-800 mb-2">{t.basicInfo}</h2>
          <p className="text-sm text-gray-500 mb-4">{t.basicInfoDesc}</p>
          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
             <div className="p-4 flex justify-between items-center border-b">
                {isEditingFullName ? (
                    <div className="flex-grow flex items-center gap-3">
                        <Input 
                            type="text" 
                            value={fullName} 
                            onChange={(e) => setFullName(e.target.value)} 
                            placeholder={t.fullName} 
                            className="flex-grow" 
                        />
                        <Button onClick={handleFullNameSave} size="sm" className="bg-[#4f46e5] hover:bg-[#4338ca] text-white">{t.save}</Button>
                        <Button onClick={() => { setIsEditingFullName(false); setFullName(user?.fullName || ''); }} size="sm" variant="outline">{t.cancel}</Button>
                    </div>
                ) : (
                    <div className="flex-grow flex items-center justify-between">
                        <p className="text-lg font-semibold text-gray-800">{fullName || t.notSet}</p>
                        <Button variant="ghost" size="icon" onClick={() => setIsEditingFullName(true)}>
                            <Edit className="w-5 h-5 text-gray-500" />
                        </Button>
                    </div>
                )}
            </div>

            <div className="flex items-center justify-between p-4 border-b last:border-b-0">
                <div className="flex items-center gap-4 cursor-pointer" onClick={handleAvatarClick}>
                    <div className="relative group">
                        <img src={user?.avatar || '/LOGO - blue line.png'} alt="Avatar" className="w-12 h-12 rounded-full bg-gray-200 object-cover" onError={(e) => { e.currentTarget.src = '/LOGO - blue line.png'; }} />
                        <div className="absolute inset-0 bg-black bg-opacity-40 flex items-center justify-center rounded-full opacity-0 group-hover:opacity-100 transition-opacity">
                            <Camera className="w-6 h-6 text-white" />
                        </div>
                    </div>
                    <div>
                        <p className="font-medium text-gray-800">{t.profilePicture}</p>
                        <div className="text-sm text-gray-500">{t.profilePictureDesc}</div>
                    </div>
                </div>
                {user?.avatar && (
                    <Button variant="ghost" size="icon" onClick={handleRemoveAvatar} className="text-red-500 hover:text-red-600 hover:bg-red-50 rounded-full">
                        <Trash2 className="w-5 h-5" />
                        <span className="sr-only">Remove avatar</span>
                    </Button>
                )}
            </div>
            <InfoRow 
              icon={User} 
              label={t.name} 
              value={user?.username || <span className='text-gray-400'>{t.notSet}</span>}
            />
            <InfoRow 
              icon={Cake} 
              label={t.birthdate} 
              value={user?.birthdate ? new Date(user.birthdate).toLocaleDateString() : <span className='text-gray-400'>{t.notSet}</span>} 
              onClick={() => openModal('birthdate', user?.birthdate || '')}
            />
            <InfoRow 
              icon={VenetianMask} 
              label={t.gender} 
              value={user?.gender || <span className='text-gray-400'>{t.notSet}</span>} 
              onClick={() => openModal('gender', user?.gender || '')}
            />
          </div>
        </section>

        <section>
          <h2 className="text-xl font-semibold text-gray-800 mb-2">{t.contactInfo}</h2>
          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <InfoRow 
              icon={Mail} 
              label={t.email} 
              value={user?.email || '-'}
            />
            <InfoRow 
              icon={Phone} 
              label={t.phone} 
              value={user?.phone || <span className='text-gray-400'>{t.notSet}</span>}
              onClick={() => openModal('phone', user?.phone || '')}
            />
          </div>
        </section>

      </div>
    </div>
  );
};

export default Account;