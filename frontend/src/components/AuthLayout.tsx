import React from 'react';
import { Link } from "react-router-dom";

interface AuthLayoutProps {
  title: string;
  children: React.ReactNode;
  page: 'login' | 'signup' | 'forgot-password' | 'reset-password' | 'verify';
}

const pageDetails = {
  login: {
    heading: "HELLO,",
    paragraph: "Welcome back to AI Contract Analyzer. Your intelligent legal assistant is ready."
  },
  signup: {
    heading: "JOIN US,",
    paragraph: "Create your account to start analyzing contracts with the power of Artificial Intelligence."
  },
  'forgot-password': {
    heading: "RECOVER,",
    paragraph: "Forgot your password? No worries. We'll help you get back into your account securely."
  },
  'reset-password': {
    heading: "SECURE,",
    paragraph: "Set a new, strong password for your account to keep it secure."
  },
  verify: {
    heading: "VERIFY,",
    paragraph: "Check your email inbox. We've sent you a code to activate your account."
  }
};

export const AuthLayout: React.FC<AuthLayoutProps> = ({ title, children, page }) => {
  const details = pageDetails[page];

  return (
    <div className="min-h-screen font-sans antialiased bg-slate-900">
      <div className="w-full h-screen grid grid-cols-1 lg:grid-cols-2">
        
        {/* Left Side: Branding & Welcome (Desktop) */}
        <div className="hidden lg:flex flex-col justify-center text-white p-12">
          <div className="max-w-md w-full mx-auto">
            <div className="space-y-4 mb-12">
                <Link to="/" className="flex items-center gap-x-4">
                  <img src="/LOGO - white line.png" alt="AGREEME Logo" className="h-10" />
                  <span className="text-2xl font-bold text-white">AGREEME</span>
                </Link>
            </div>
            <div className="space-y-6">
                <h2 className="text-7xl font-bold tracking-tighter">{details.heading}</h2>
                <p className="text-lg text-slate-400">{details.paragraph}</p>
            </div>
          </div>
        </div>

        {/* Right Side: Form */}
        <main className="bg-white lg:rounded-l-[40px] shadow-2xl flex items-center justify-center p-6 sm:p-8 lg:p-12 relative overflow-y-auto custom-scrollbar">
          {/* Logo for Mobile */}
          <div className="absolute top-8 left-8 lg:hidden">
             <Link to="/" className="flex items-center gap-x-3">
                <img src="/LOGO - blue line.png" alt="AGREEME Logo" className="h-8" />
                <span className="text-xl font-bold text-slate-900">AGREEME</span>
             </Link>
          </div>

          <div className="max-w-md w-full pt-16 lg:pt-0">
            <h2 className="text-4xl font-bold text-slate-800 mb-8">{title}</h2>
            {children}
          </div>
        </main>
        
      </div>
    </div>
  );
};