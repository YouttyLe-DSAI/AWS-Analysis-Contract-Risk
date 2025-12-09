import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useNavigate } from "react-router-dom";
import { useAuth } from "@/contexts/AuthContext";
import { useToast } from "@/components/ui/use-toast";
import { z } from "zod";
import { AuthLayout } from "@/components/AuthLayout";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertTriangle } from "lucide-react";
import { signIn } from 'aws-amplify/auth';

const loginSchema = z.object({
  username: z.string().min(1, "Vui lòng nhập email"), 
  password: z.string().min(1, "Vui lòng nhập mật khẩu"),
});

const Login = () => {
  const navigate = useNavigate();
  const { user } = useAuth(); 
  const { toast } = useToast();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (user) navigate("/dashboard");
  }, [user, navigate]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    const validation = loginSchema.safeParse({ username, password });
    if (!validation.success) {
      setError(validation.error.errors[0].message);
      return;
    }

    setLoading(true);
    try {
      const { isSignedIn, nextStep } = await signIn({ 
        username: username, 
        password: password 
      });

      if (isSignedIn) {
        toast({ title: "Đăng nhập thành công!" });
        navigate("/dashboard");
      } else {
        if (nextStep.signInStep === 'CONFIRM_SIGN_UP') {
            toast({ 
                title: "Cần xác thực email", 
                description: "Vui lòng nhập mã OTP để kích hoạt tài khoản." 
            });
            navigate("/verify-email", { state: { email: username } });
        }
      }
    } catch (err: any) {
      console.error("Login error:", err);
      
      let msg = err.message;
      
      if (msg.includes("User is not confirmed")) {
          toast({
              variant: "destructive",
              title: "Tài khoản chưa kích hoạt",
              description: "Hệ thống đang chuyển bạn đến trang xác thực..."
          });
          navigate("/verify-email", { state: { email: username } });
          return;
      }

      if (msg.includes("Incorrect username or password")) msg = "Sai email hoặc mật khẩu.";
      
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <AuthLayout title="Đăng nhập" page="login">
      <form onSubmit={handleSubmit} className="space-y-5">
        {error && (
            <Alert variant="destructive">
                <AlertTriangle className="h-4 w-4" />
                <AlertTitle>Lỗi đăng nhập</AlertTitle>
                <AlertDescription>{error}</AlertDescription>
            </Alert>
        )}
        <div className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="username" className="font-semibold text-gray-700">Email <span className="text-red-500">*</span></Label>
            <Input id="username" placeholder="Nhập email của bạn" value={username} onChange={(e) => setUsername(e.target.value)} className="h-12 rounded-lg border-gray-300 focus:border-indigo-500 focus:ring-indigo-500" required/>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="password" className="font-semibold text-gray-700">Mật khẩu <span className="text-red-500">*</span></Label>
            <Input id="password" type="password" placeholder="Nhập mật khẩu" value={password} onChange={(e) => setPassword(e.target.value)} className="h-12 rounded-lg border-gray-300 focus:border-indigo-500 focus:ring-indigo-500" required/>
          </div>
        </div>

        <div className="flex justify-between items-center text-sm">
            <div>
                <span className="text-gray-600">Chưa có tài khoản? </span>
                <span className="font-semibold text-[#4f46e5] cursor-pointer hover:underline" onClick={() => navigate("/signup")}>Đăng ký</span>
            </div>
            <span className="font-semibold text-[#4f46e5] cursor-pointer hover:underline" onClick={() => navigate("/forgot-password")}>Quên mật khẩu?</span>
        </div>

        <Button type="submit" className="w-full h-12 bg-[#4f46e5] hover:bg-[#3b5bdb] text-white rounded-lg font-bold text-base" disabled={loading}>
          {loading ? "Đang đăng nhập..." : "Đăng nhập"}
        </Button>

      </form>
    </AuthLayout>
  );
};

export default Login;