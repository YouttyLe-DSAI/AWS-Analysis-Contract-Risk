import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useNavigate } from "react-router-dom";
import { resetPassword } from 'aws-amplify/auth';
import { AuthLayout } from "@/components/AuthLayout";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle, Loader2 } from "lucide-react";

const ForgotPassword = () => {
  const navigate = useNavigate();
  
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!email) {
      setError("Vui lòng nhập địa chỉ email của bạn.");
      return;
    }

    setLoading(true);
    try {
      const output = await resetPassword({ username: email });
      const { nextStep } = output;

      if (nextStep.resetPasswordStep === 'CONFIRM_RESET_PASSWORD_WITH_CODE') {
        navigate(`/reset-password?email=${encodeURIComponent(email)}`);
      }
      
    } catch (err: any) {
      console.error("Reset password error:", err);
      setError(err.message || "Đã xảy ra lỗi. Vui lòng kiểm tra lại email.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <AuthLayout title="Quên Mật khẩu" page="forgot-password">
        <p className="text-slate-600 text-center mb-6">
            Nhập email của bạn và chúng tôi sẽ gửi mã xác nhận để đặt lại mật khẩu.
        </p>
      
      <form onSubmit={handleSubmit} className="space-y-5">
        {error && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Lỗi</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        
        <div className="space-y-1.5">
          <Label htmlFor="email" className="font-semibold text-slate-700">
            Email <span className="text-red-500">*</span>
          </Label>
          <Input 
            id="email" 
            type="email" 
            placeholder="Nhập email của bạn" 
            value={email} 
            onChange={(e) => setEmail(e.target.value)} 
            className="h-12 rounded-lg" 
            required 
          />
        </div>

        <div className="text-center text-sm pt-2">
            <span className="text-slate-600">Nhớ mật khẩu? </span>
            <span 
                className="font-semibold text-indigo-600 cursor-pointer hover:underline" 
                onClick={() => navigate("/login")}
            >
                Đăng nhập
            </span>
        </div>

        <Button 
            type="submit" 
            className="w-full h-12 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg font-bold text-base" 
            disabled={loading}
        >
          {loading ? (
             <><Loader2 className="mr-2 h-4 w-4 animate-spin"/> Đang gửi...</>
          ) : (
             "Gửi mã xác nhận"
          )}
        </Button>
      </form>
    </AuthLayout>
  );
};

export default ForgotPassword;