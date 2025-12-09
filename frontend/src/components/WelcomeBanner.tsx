
import { useAuth } from "@/contexts/AuthContext";
import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Zap } from "lucide-react";

export function WelcomeBanner() {
  const { user } = useAuth();
  const navigate = useNavigate();

  const getDisplayName = () => {
    if (user?.fullName) {
      return user.fullName;
    }
    if (user?.username) {
      return user.username;
    }
    if (user?.email) {
      const name = user.email.split('@')[0];
      return name.charAt(0).toUpperCase() + name.slice(1);
    }
    return "Admin";
  };

  return (
    <div className="bg-gradient-to-r from-indigo-600 to-purple-600 rounded-3xl p-10 text-white shadow-xl relative overflow-hidden mb-10">
      <div className="relative z-10 max-w-2xl">
        <h1 className="text-3xl font-bold mb-4">Welcome back, {getDisplayName()}!</h1>
        <h2 className="text-xl font-medium opacity-90 mb-6">Start your automated contract analysis.</h2>
        <p className="opacity-80 mb-8 leading-relaxed max-w-lg">Use AI to scan for risks, manage templates, and ensure compliance across all your legal documents in seconds.</p>
        <div className="flex gap-4">
          <Button onClick={() => navigate('/deep-analysis')} className="bg-white text-indigo-600 hover:bg-slate-100 font-bold px-6 py-5 rounded-xl flex items-center gap-2 border-none"><Zap size={18} fill="currentColor"/> Quick Review</Button>
          <Button onClick={() => navigate('/library')} className="bg-transparent border border-white/30 text-white hover:bg-white/10 px-6 py-5 rounded-xl font-medium">Templates</Button>
        </div>
      </div>
      {/* Using img tag for better control and visibility */}
      <img 
        src="/LOGO - white line.png"
        alt="Watermark"
        className="absolute top-1/2 right-10 transform -translate-y-1/2 w-80 h-80 opacity-20 mix-blend-lighten pointer-events-none select-none"
      />
    </div>
  );
}
