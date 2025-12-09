import React, { useState, ReactNode } from 'react';
import { Sidebar } from './Sidebar';
import { cn } from '@/lib/utils';

interface DashboardLayoutProps {
  children: ReactNode;
}

export function DashboardLayout({ children }: DashboardLayoutProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [isHoverExpanded, setIsHoverExpanded] = useState(false);

  const isSidebarVisible = !isCollapsed || isHoverExpanded;

  return (
    <div className="flex h-screen w-full bg-[#f4f5f7]">
      <div
        className={cn(
          "h-full bg-white transition-all duration-300 ease-in-out z-30 flex-shrink-0 shadow-sm",
          isSidebarVisible ? "w-64" : "w-20"
        )}
        onMouseEnter={() => {
          if (isCollapsed) {
            setIsHoverExpanded(true);
          }
        }}
        onMouseLeave={() => {
          if (isCollapsed) {
            setIsHoverExpanded(false);
          }
        }}
      >
        <Sidebar
            isCollapsed={!isSidebarVisible}
            setIsCollapsed={setIsCollapsed}
            isPermanentlyCollapsed={isCollapsed}
        />
      </div>

      {/* Main content area that grows and allows its children to handle their own scrolling */}
      <main className="flex-1 flex flex-col min-w-0 overflow-y-auto">
        {children}
      </main>
    </div>
  );
}
