import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useNavigate, useSearchParams } from "react-router-dom";
import { confirmResetPassword } from 'aws-amplify/auth';
import { AuthLayout } from "@/components/AuthLayout";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle, CheckCircle, Loader2 } from "lucide-react";
import { z } from "zod";

const resetPasswordSchema = z.object({
  password: z.string().min(8, "Mật khẩu phải có ít nhất 8 ký tự"),
  confirmPassword: z.string(),
}).refine((data) => data.password === data.confirmPassword, {
  message: "Mật khẩu xác nhận không khớp",
  path: ["confirmPassword"],
});

const ResetPassword = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();  
  const emailFromUrl = searchParams.get('email') || '';
  const [email, setEmail] = useState(emailFromUrl);
  const [code, setCode] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isSuccess, setIsSuccess] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    const validation = resetPasswordSchema.safeParse({ password, confirmPassword });
    if (!validation.success) {
      setError(validation.error.errors[0].message);
      return;
    }

    if (!email || !code) {
        setError("Vui lòng nhập đầy đủ Email và Mã xác nhận.");
        return;
    }

    setLoading(true);
    try {
      await confirmResetPassword({
        username: email,
        confirmationCode: code,
        newPassword: password
      });
      
      setIsSuccess(true);
    } catch (err: any) {
      console.error("Confirm reset password error:", err);
      setError(err.message || "Mã xác nhận không đúng hoặc đã hết hạn.");
    } finally {
      setLoading(false);
    }
  };

  if (isSuccess) {
    return (
      <AuthLayout title="Thành công" page="reset-password">
        <div className="text-center space-y-6">
            <div className="flex justify-center">
                 <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center">
                    <CheckCircle className="w-8 h-8 text-green-600" />
                </div>
            </div>
            <h3 className="text-xl font-semibold text-slate-800">Mật khẩu đã được đặt lại</h3>
            <p className="text-slate-600">
                Mật khẩu của bạn đã được cập nhật thành công. Hãy đăng nhập lại ngay.
            </p>
            <Button onClick={() => navigate("/login")} className="w-full h-12 bg-indigo-600 hover:bg-indigo-700 text-white font-bold">
                Đăng nhập ngay
            </Button>
        </div>
      </AuthLayout>
    );
  }

  return (
    <AuthLayout title="Đặt lại Mật khẩu" page="reset-password">
      <form onSubmit={handleSubmit} className="space-y-5">
        {error && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Lỗi</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        
        <div className="space-y-4">
          {/*Ô nhập Email*/}
          <div className="space-y-1.5">
            <Label htmlFor="email" className="font-semibold text-slate-700">Email</Label>
            <Input 
                id="email" 
                value={email} 
                onChange={(e) => setEmail(e.target.value)} 
                className="h-12 rounded-lg bg-gray-50" 
                placeholder="Nhập email của bạn"
            />
          </div>

          {/*Ô nhập Mã Code*/}
          <div className="space-y-1.5">
            <Label htmlFor="code" className="font-semibold text-slate-700">Mã xác nhận (Code) <span className="text-red-500">*</span></Label>
            <Input 
                id="code" 
                value={code} 
                onChange={(e) => setCode(e.target.value)} 
                className="h-12 rounded-lg" 
                placeholder="Nhập mã 6 số từ email" 
                required 
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="password" className="font-semibold text-slate-700">Mật khẩu mới <span className="text-red-500">*</span></Label>
            <Input 
                id="password" 
                type="password" 
                placeholder="Nhập mật khẩu mới" 
                value={password} 
                onChange={(e) => setPassword(e.target.value)} 
                className="h-12 rounded-lg" 
                required 
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="confirmPassword" className="font-semibold text-slate-700">Xác nhận mật khẩu <span className="text-red-500">*</span></Label>
            <Input 
                id="confirmPassword" 
                type="password" 
                placeholder="Nhập lại mật khẩu mới" 
                value={confirmPassword} 
                onChange={(e) => setConfirmPassword(e.target.value)} 
                className="h-12 rounded-lg" 
                required 
            />
          </div>
        </div>

        <Button 
            type="submit" 
            className="w-full h-12 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg font-bold text-base" 
            disabled={loading}
        >
          {loading ? (
             <><Loader2 className="mr-2 h-4 w-4 animate-spin"/> Đang cập nhật...</>
          ) : (
             "Đặt lại Mật khẩu"
          )}
        </Button>
      </form>
    </AuthLayout>
  );
};

export default ResetPassword;