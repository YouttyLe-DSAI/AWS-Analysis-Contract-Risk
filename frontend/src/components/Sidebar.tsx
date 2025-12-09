import React from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { cn } from "@/lib/utils";
import { 
  LayoutDashboard, FileText, Search, LogOut, FolderOpen, 
  BookCopy, User, ChevronsLeft
} from "lucide-react";
import { useAuth } from "@/contexts/AuthContext";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Button } from "@/components/ui/button";

interface SidebarProps {
  isCollapsed: boolean; // Reflects the current visual state (icon-only or expanded)
  setIsCollapsed: (isCollapsed: boolean) => void; // Toggles the permanent collapsed state
  isPermanentlyCollapsed: boolean; // The actual permanent state for button logic
}

export function Sidebar({ isCollapsed, setIsCollapsed, isPermanentlyCollapsed }: SidebarProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, signOut } = useAuth();

  const menuItems = [
    { icon: LayoutDashboard, label: "Home", path: "/dashboard" },
    { icon: Search, label: "Analysis", path: "/deep-analysis" },
    { icon: FileText, label: "Templates", path: "/templates" },
    { icon: BookCopy, label: "Library", path: "/library" },
    { icon: FolderOpen, label: "Inspections", path: "/inspections" },
    { icon: User, label: "Account", path: "/account" },
  ];

  const handleSignOut = async (e: React.MouseEvent) => {
    e.stopPropagation();
    await signOut();
    navigate("/login");
  };

  return (
    <TooltipProvider delayDuration={0}>
      <aside className="h-full bg-white flex flex-col border-r border-gray-200 shadow-sm shrink-0 z-20 relative p-2 space-y-2">
        {/* 1. Logo Area */}
        <div className={cn("h-16 flex items-center border-b border-gray-100 shrink-0", isCollapsed ? "justify-center" : "px-4")}>
            <Link to="/" className="flex items-center gap-2">
                <img src="/LOGO - blue line.png" alt="AGREEME Logo" className="h-9 w-9 shrink-0" />
                <span className={cn("font-bold text-xl text-[#1e1b4b] tracking-tight transition-opacity duration-200 whitespace-nowrap", isCollapsed && "opacity-0 hidden")}>Agreeme</span>
            </Link>
        </div>

        {/* 2. Menu Items */}
        <nav className="flex-1 px-3.5 space-y-1.5 overflow-y-auto mt-6">
          {menuItems.map((item) => {
            const Icon = item.icon;
            const isActive = location.pathname.startsWith(item.path);

            const linkContent = (
              <>
                {isActive && !isCollapsed && (
                    <div className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-6 bg-[#4F46E5] rounded-r-full"></div>
                )}
                <Icon 
                  className={cn(
                    "w-5 h-5 shrink-0 transition-colors", 
                    isActive ? "text-[#4F46E5]" : "text-slate-400 group-hover:text-slate-600"
                  )} 
                />
                <span className={cn("transition-opacity duration-200 whitespace-nowrap", isCollapsed && "opacity-0 hidden")}>
                  {item.label}
                </span>
                 {isCollapsed && isActive && (
                  <div className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-4 h-1 bg-[#4F46E5] rounded-full"></div>
                )}
              </>
            );
            
            const linkClasses = cn(
                "flex items-center gap-3 h-11 px-3 rounded-xl text-[14px] font-medium transition-all duration-200 group relative",
                isActive 
                  ? "bg-[#EEF2FF] text-[#4F46E5]" 
                  : "text-slate-500 hover:bg-slate-50 hover:text-slate-900",
                isCollapsed && "justify-center"
            );

            return isCollapsed ? (
              <Tooltip key={item.path}>
                <TooltipTrigger asChild>
                    <Link to={item.path} className={linkClasses}>{linkContent}</Link>
                </TooltipTrigger>
                <TooltipContent side="right" className="ml-2">
                  <p>{item.label}</p>
                </TooltipContent>
              </Tooltip>
            ) : (
              <Link key={item.path} to={item.path} className={linkClasses}>{linkContent}</Link>
            );
          })}
        </nav>
        
        {/* Collapse/Expand Toggle Button */}
        <div className="absolute top-16 -right-3">
             <Button size="icon" variant="secondary" className="rounded-full w-6 h-6 bg-white border-2 hover:bg-slate-100" onClick={() => setIsCollapsed(!isPermanentlyCollapsed)}>
                <ChevronsLeft size={16} className={cn("text-slate-500 transition-transform", isPermanentlyCollapsed && "rotate-180")} />
            </Button>
        </div>

        {/* 3. Footer Sidebar */}
        <div className={cn("p-4 border-t border-gray-100", isCollapsed && "p-2")}>
          <div onClick={() => navigate('/account')} className={cn("flex items-center gap-3 px-3 py-2 rounded-xl hover:bg-slate-50 cursor-pointer group transition-colors", isCollapsed && "justify-center")}>
            <Avatar className="h-9 w-9 border border-slate-200 shrink-0">
              <AvatarImage src={user?.avatar || ''} alt={user?.username || 'User'} onError={(e) => { e.currentTarget.src = '/LOGO - blue line.png'; }} />
              <AvatarFallback className="bg-[#4F46E5] text-white text-xs font-bold">
                {user?.username?.charAt(0).toUpperCase() || "U"}
              </AvatarFallback>
            </Avatar>
            <div className={cn("flex-1 min-w-0 transition-opacity duration-200", isCollapsed && "opacity-0 hidden")}>
              <p className="text-sm font-semibold text-slate-700 truncate group-hover:text-[#4F46E5] transition-colors">
                {user?.username || "User"}
              </p>
              <p className="text-[11px] text-slate-400 truncate">{user?.email || "user@example.com"}</p>
            </div>
            <LogOut 
              className={cn("w-4 h-4 text-slate-400 group-hover:text-red-500 transition-colors shrink-0", isCollapsed && "hidden")} 
              onClick={handleSignOut}
            />
          </div>
        </div>
      </aside>
    </TooltipProvider>
  );
}
