import { useEffect } from "react";
import { Button } from "@/components/ui/button";
import { useNavigate } from "react-router-dom";
import { ShieldCheck, Users, Globe, Zap, ArrowRight } from "lucide-react";

export const About = () => {
  const navigate = useNavigate();

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  return (
    <div className="min-h-screen bg-white font-sans pt-24 pb-20 text-slate-900">
      
      {/* 1. HERO SECTION: Clean & Direct */}
      <div className="max-w-5xl mx-auto px-6 text-center mb-24">
        <h1 className="text-4xl md:text-6xl font-extrabold tracking-tight text-[#1A1C4B] mb-6">
          Building the Standard for <br />
          <span className="text-[#496DFF]">Digital Trust.</span>
        </h1>
        <p className="text-xl text-slate-500 max-w-2xl mx-auto leading-relaxed">
          AGREEME simplifies the world's most important interactions. 
          We provide the secure infrastructure where agreements happen.
        </p>
      </div>

      {/* 2. STATS SECTION: Numbers build trust */}
      <div className="bg-slate-50 border-y border-slate-100 py-16 mb-24">
        <div className="max-w-6xl mx-auto px-6 grid grid-cols-2 md:grid-cols-4 gap-8 text-center">
          <div>
            <div className="text-4xl font-bold text-[#496DFF] mb-2">100k+</div>
            <div className="text-sm font-medium text-slate-500 uppercase tracking-wider">Active Users</div>
          </div>
          <div>
            <div className="text-4xl font-bold text-[#496DFF] mb-2">2M+</div>
            <div className="text-sm font-medium text-slate-500 uppercase tracking-wider">Documents Signed</div>
          </div>
          <div>
            <div className="text-4xl font-bold text-[#496DFF] mb-2">99.9%</div>
            <div className="text-sm font-medium text-slate-500 uppercase tracking-wider">Uptime SLA</div>
          </div>
          <div>
            <div className="text-4xl font-bold text-[#496DFF] mb-2">50+</div>
            <div className="text-sm font-medium text-slate-500 uppercase tracking-wider">Countries</div>
          </div>
        </div>
      </div>

      {/* 3. MISSION SECTION: Simple Grid Layout */}
      <div className="max-w-6xl mx-auto px-6 mb-24">
        <div className="grid md:grid-cols-2 gap-16 items-center">
          <div>
            <div className="w-12 h-1 bg-[#496DFF] mb-6"></div>
            <h2 className="text-3xl font-bold text-[#1A1C4B] mb-6">
              Why we exist
            </h2>
            <div className="space-y-4 text-lg text-slate-600 leading-relaxed">
              <p>
                The modern world moves fast, but paperwork holds it back. We noticed that while communication became instant, agreements remained stuck in the past.
              </p>
              <p>
                Our mission is to remove friction from trust. We believe that securing an agreement should be as easy as sending an email, but as secure as a bank vault.
              </p>
            </div>
          </div>
          
          {/* Feature Grid bên phải */}
          <div className="grid grid-cols-1 gap-6">
             <div className="flex items-start gap-4 p-6 rounded-2xl bg-white border border-slate-100 shadow-sm hover:shadow-md transition-shadow">
                <div className="bg-blue-50 p-3 rounded-lg text-[#496DFF]">
                  <ShieldCheck className="w-6 h-6" />
                </div>
                <div>
                  <h3 className="font-bold text-[#1A1C4B] text-lg">Bank-Grade Security</h3>
                  <p className="text-slate-500 mt-1">End-to-end encryption ensures only you and your recipients see the data.</p>
                </div>
             </div>

             <div className="flex items-start gap-4 p-6 rounded-2xl bg-white border border-slate-100 shadow-sm hover:shadow-md transition-shadow">
                <div className="bg-blue-50 p-3 rounded-lg text-[#496DFF]">
                  <Zap className="w-6 h-6" />
                </div>
                <div>
                  <h3 className="font-bold text-[#1A1C4B] text-lg">Seamless Workflow</h3>
                  <p className="text-slate-500 mt-1">Integrate with your favorite tools. Automate reminders and storage.</p>
                </div>
             </div>
          </div>
        </div>
      </div>

      {/* 4. VALUES SECTION */}
      <div className="max-w-6xl mx-auto px-6 mb-24">
        <div className="text-center mb-16">
          <h2 className="text-3xl font-bold text-[#1A1C4B]">Our Core Values</h2>
        </div>
        <div className="grid md:grid-cols-3 gap-8">
          {[
            {
              icon: Globe,
              title: "Universal Access",
              desc: "We build tools that are accessible to everyone, from freelancers to enterprises."
            },
            {
              icon: Users,
              title: "Human First",
              desc: "Technology should serve people, not complicate their lives. We design for clarity."
            },
            {
              icon: ShieldCheck,
              title: "Integrity",
              desc: "We never sell your data. Your privacy is the foundation of our business model."
            }
          ].map((item, index) => (
            <div key={index} className="bg-slate-50 p-8 rounded-2xl text-center hover:bg-white hover:shadow-xl hover:-translate-y-1 transition-all duration-300 border border-transparent hover:border-slate-100">
              <item.icon className="w-10 h-10 mx-auto text-[#496DFF] mb-4" />
              <h3 className="font-bold text-[#1A1C4B] text-xl mb-3">{item.title}</h3>
              <p className="text-slate-500 leading-relaxed">{item.desc}</p>
            </div>
          ))}
        </div>
      </div>

      {/* 5. BOTTOM CTA: Simple & Professional */}
      <div className="max-w-4xl mx-auto px-6 text-center">
        <div className="bg-[#1A1C4B] rounded-3xl p-12 md:p-16 text-white overflow-hidden relative">
          <div className="absolute top-0 right-0 w-64 h-64 bg-[#496DFF] opacity-10 rounded-full blur-3xl -mr-32 -mt-32"></div>
          <h2 className="text-3xl font-bold mb-6 relative z-10">Start simplifying your agreements today.</h2>
          <p className="text-blue-200 mb-8 max-w-xl mx-auto relative z-10">
            Join the community of professionals who trust AGREEME. No credit card required for the free tier.
          </p>
          <div className="relative z-10">
            <Button 
              size="lg" 
              className="bg-[#496DFF] hover:bg-[#3b5bdb] text-white h-12 px-8 rounded-full font-semibold"
              onClick={() => navigate("/signup")}
            >
              Get Started Free <ArrowRight className="ml-2 w-4 h-4" />
            </Button>
          </div>
        </div>
      </div>

    </div>
  );
};

export default About;