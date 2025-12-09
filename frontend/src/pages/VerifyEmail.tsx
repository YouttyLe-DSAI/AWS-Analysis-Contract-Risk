// src/pages/VerifyEmail.tsx
import { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { confirmSignUp, resendSignUpCode } from 'aws-amplify/auth';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/components/ui/use-toast";
import { AuthLayout } from "@/components/AuthLayout";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle, Mail } from "lucide-react";

const VerifyEmail = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const { toast } = useToast();
  const defaultEmail = location.state?.email || "";
  const [email, setEmail] = useState(defaultEmail);
  const [code, setCode] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleVerify = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);

    try {
      const { isSignUpComplete, nextStep } = await confirmSignUp({
        username: email,
        confirmationCode: code
      });

      if (isSignUpComplete) {
        toast({
          title: "Xác thực thành công!",
          description: "Tài khoản của bạn đã được kích hoạt. Vui lòng đăng nhập.",
        });
        navigate("/login");
      } else {
        setError("Xác thực chưa hoàn tất. Vui lòng thử lại.");
      }

    } catch (err: any) {
      console.error("Verify error:", err);
      let msg = err.message;
      if (msg.includes("Code mismatch")) msg = "Mã xác thực không đúng.";
      if (msg.includes("ExpiredCodeException")) msg = "Mã xác thực đã hết hạn.";
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  const handleResendCode = async () => {
    if (!email) {
      setError("Vui lòng nhập email để gửi lại mã.");
      return;
    }
    try {
      await resendSignUpCode({ username: email });
      toast({
        title: "Đã gửi lại mã",
        description: "Vui lòng kiểm tra hộp thư đến (và spam) của bạn.",
      });
    } catch (err: any) {
      toast({
        variant: "destructive",
        title: "Gửi mã thất bại",
        description: err.message,
      });
    }
  };

  return (
    <AuthLayout title="Xác thực Email" page="verify">
      <div className="text-center mb-6">
        <div className="bg-blue-50 w-12 h-12 rounded-full flex items-center justify-center mx-auto mb-4">
            <Mail className="h-6 w-6 text-blue-600" />
        </div>
        <p className="text-gray-600 text-sm">
          Chúng tôi đã gửi mã gồm 6 chữ số đến email của bạn.
        </p>
      </div>

      <form onSubmit={handleVerify} className="space-y-5">
        {error && (
            <Alert variant="destructive">
                <AlertTriangle className="h-4 w-4" />
                <AlertTitle>Lỗi xác thực</AlertTitle>
                <AlertDescription>{error}</AlertDescription>
            </Alert>
        )}

        <div className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="email" className="font-semibold text-gray-700">Email</Label>
            <Input 
              id="email" 
              type="email" 
              value={email} 
              onChange={(e) => setEmail(e.target.value)} 
              disabled={!!defaultEmail} 
              className="h-12 bg-gray-50"
            />
          </div>
          
          <div className="space-y-1.5">
            <Label htmlFor="code" className="font-semibold text-gray-700">Mã xác thực (OTP)</Label>
            <Input 
              id="code" 
              placeholder="Nhập 6 số (VD: 123456)" 
              value={code} 
              onChange={(e) => setCode(e.target.value)} 
              maxLength={6}
              className="h-12 text-center text-lg tracking-widest font-mono" 
              required
            />
          </div>
        </div>

        <Button type="submit" className="w-full h-12 bg-[#4f46e5] hover:bg-[#3b5bdb] text-white rounded-lg font-bold text-base" disabled={loading}>
          {loading ? "Đang xác thực..." : "Xác nhận"}
        </Button>

        <div className="text-center mt-4">
            <button 
                type="button"
                onClick={handleResendCode}
                className="text-sm text-[#4f46e5] font-semibold hover:underline"
            >
                Chưa nhận được mã? Gửi lại
            </button>
        </div>
      </form>
    </AuthLayout>
  );
};

export default VerifyEmail;