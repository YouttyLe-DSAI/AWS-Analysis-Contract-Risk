import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { FileCheck, FileSearch, FolderOpen, ArrowRight, ShieldCheck, Zap } from "lucide-react";
import { useTranslation } from "@/lib/translations";
import { useNavigate } from "react-router-dom";
import { motion } from "framer-motion";
import { useAuth } from "@/contexts/AuthContext";

export default function Home() {
  const { t, language } = useTranslation();
  const navigate = useNavigate();
  const { user } = useAuth();

  const features = [
    {
      icon: FileCheck,
      title: t.quickReviewTitle,
      description: t.quickReviewDesc,
      link: "/deep-analysis",
    },
    {
      icon: FileSearch,
      title: t.deepAnalysisTitle,
      description: t.deepAnalysisDesc,
      link: "/deep-analysis",
    },
    {
      icon: FolderOpen,
      title: t.templatesTitle,
      description: t.templatesDesc,
      link: "/templates",
    },
  ];

  const handleLearnMoreClick = () => {
    document.getElementById('features')?.scrollIntoView({ behavior: 'smooth' });
  };

  return (
    <div className="min-h-screen bg-white font-sans pt-24 text-slate-900">
      
      {/* 1. HERO SECTION */}
      <section className="w-full bg-white pb-24">
        <div className="container mx-auto px-6 text-center max-w-5xl">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8 }}
            className="space-y-8"
          >
            <h1 className="text-5xl md:text-7xl font-extrabold leading-tight text-[#1e1b4b]">
              {t.heroTitle} <br className="hidden md:block" />
              <span className="text-[#4f46e5] relative inline-block">
                AGREEME
              </span>
            </h1>
            <p className="text-xl md:text-2xl text-slate-500 max-w-2xl mx-auto leading-relaxed">
              {t.heroSubtitle}. {t.heroDescription}
            </p>
            <div className="flex flex-col sm:flex-row gap-4 pt-8 justify-center items-center"> 
              <Button
                size="lg"
                className="h-14 px-8 bg-[#4f46e5] hover:bg-[#4338ca] text-white font-semibold rounded-full shadow-lg shadow-indigo-200 transition-all hover:scale-105 text-lg" 
                onClick={() => navigate("/signup")}
              >
                {t.getStarted} <ArrowRight className="ml-2 w-5 h-5" />
              </Button>
              <Button
                size="lg"
                variant="outline"
                className="h-14 px-8 bg-transparent border-slate-300 hover:bg-slate-100 text-slate-700 font-semibold rounded-full shadow-lg transition-all hover:scale-105 text-lg"
                onClick={handleLearnMoreClick}
              >
                {language === "vi" ? "Tìm hiểu thêm" : "Learn More"}
              </Button>
            </div>
          </motion.div>
        </div>
      </section>

      {/* 2. FEATURES SECTION */}
      <section id="features" className="py-24 bg-slate-50 border-y border-slate-100">
        <div className="container mx-auto px-6">
          <div className="text-center mb-16 max-w-3xl mx-auto">
            <h2 className="text-3xl md:text-4xl font-bold mb-4 text-[#1e1b4b]">
              {language === "vi" ? "Tính năng vượt trội" : "Everything you need"}
            </h2>
            <p className="text-slate-500 text-lg">
               Powerful tools designed to make your contract workflow seamless and secure.
            </p>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 max-w-6xl mx-auto">
            {features.map((feature, idx) => {
              const Icon = feature.icon;
              return (
                <motion.div
                  key={idx}
                  initial={{ opacity: 0, y: 20 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  viewport={{ once: true }}
                  transition={{ delay: idx * 0.1 }}
                >
                  <Card
                    className="h-full p-8 border border-slate-100 hover:border-[#4f46e5]/30 shadow-sm hover:shadow-xl hover:-translate-y-1 transition-all duration-300 cursor-pointer group bg-white rounded-3xl"
                    onClick={() => navigate(feature.link)}
                  >
                    <div className="w-14 h-14 rounded-2xl bg-indigo-50 flex items-center justify-center mb-6 group-hover:bg-[#4f46e5] transition-colors duration-300">
                      <Icon className="w-7 h-7 text-[#4f46e5] group-hover:text-white transition-colors duration-300" />
                    </div>
                    <h3 className="text-xl font-bold mb-3 text-[#1e1b4b]">
                      {feature.title}
                    </h3>
                    <p className="text-slate-500 mb-6 leading-relaxed">
                      {feature.description}
                    </p>
                    <div className="flex items-center text-[#4f46e5] font-bold text-sm group-hover:underline decoration-2 underline-offset-4">
                      {language === "vi" ? "Khám phá ngay" : "Explore Feature"}
                      <ArrowRight className="ml-2 w-4 h-4 group-hover:translate-x-1 transition-transform" />
                    </div>
                  </Card>
                </motion.div>
              );
            })}
          </div>
        </div>
      </section>

      {/* 3. FAQ SECTION */}
      <section className="py-24 bg-white">
        <div className="container mx-auto px-6 max-w-3xl">
          <div className="text-center mb-12">
             <div className="inline-block p-3 rounded-xl bg-indigo-50 text-[#4f46e5] mb-4">
                <ShieldCheck className="w-6 h-6" />
             </div>
             <h2 className="text-3xl md:text-4xl font-bold text-[#1e1b4b]">
              {t.faqTitle}
            </h2>
          </div>
          <Accordion type="single" collapsible className="space-y-2">
            {[1, 2, 3, 4, 5].map((item) => (
              <AccordionItem
                key={item}
                value={`item-${item}`}
                className="border border-slate-100 rounded-2xl px-6 bg-slate-50 data-[state=open]:bg-white data-[state=open]:shadow-md transition-all duration-200"
              >
                <AccordionTrigger className="text-lg font-semibold text-[#1e1b4b] hover:text-[#4f46e5] py-6 text-left hover:no-underline">
                  {t[`faqQuestion${item}`]}
                </AccordionTrigger>
                <AccordionContent className="text-slate-500 text-base pb-6 leading-relaxed">
                  {t[`faqAnswer${item}`]}
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        </div>
      </section>

      {/* 4. CTA SECTION */}
      <section className="py-12 px-4 md:px-6 pb-24">
        <div className="container mx-auto max-w-5xl">
          <div className="bg-[#1e1b4b] rounded-[40px] p-12 md:p-20 text-center text-white relative overflow-hidden shadow-2xl">
            <div className="absolute top-0 right-0 w-64 h-64 bg-[#4f46e5] opacity-20 rounded-full blur-3xl -mr-32 -mt-32"></div>
            <div className="absolute bottom-0 left-0 w-64 h-64 bg-[#4f46e5] opacity-20 rounded-full blur-3xl -ml-32 -mb-32"></div>
            <div className="relative z-10">
              <div className="inline-flex items-center justify-center p-3 mb-6 bg-white/10 rounded-2xl backdrop-blur-sm border border-white/10">
                 <Zap className="w-6 h-6 text-[#4f46e5]" />
              </div>
              <h2 className="text-3xl md:text-5xl font-bold mb-6 tracking-tight">
                {language === "vi" ? "Sẵn sàng bắt đầu?" : "Ready to get started?"}
              </h2>
              <p className="text-indigo-200 text-lg md:text-xl mb-10 max-w-xl mx-auto leading-relaxed">
                {language === "vi"
                  ? "Bắt đầu phân tích và tạo hợp đồng chuyên nghiệp ngay hôm nay với công nghệ AI hàng đầu."
                  : "Start analyzing and creating professional contracts today with our leading AI technology."}
              </p>
              <div className="flex flex-col sm:flex-row justify-center items-center gap-x-6 gap-y-4">
                {!user ? (
                  <Button
                    size="lg"
                    className="bg-[#4f46e5] hover:bg-[#4338ca] text-white h-14 px-10 rounded-full font-bold text-lg shadow-lg"
                    onClick={() => navigate("/signup")}
                  >
                    {language === "vi" ? "Tạo tài khoản" : "Create Account"}
                  </Button>
                ) : (
                  <Button
                    size="lg"
                    className="bg-[#4f46e5] hover:bg-[#4338ca] text-white h-14 px-10 rounded-full font-bold text-lg shadow-lg"
                    onClick={() => navigate("/dashboard")}
                  >
                    {language === "vi" ? "Đi tới Bảng điều khiển" : "Go to Dashboard"}
                  </Button>
                )}
                <span
                  onClick={() => navigate("/about")}
                  className="font-medium text-white hover:underline cursor-pointer"
                >
                  {language === "vi" ? "Về chúng tôi" : "About Us"}
                </span>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

