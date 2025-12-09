
import React, { ReactNode } from 'react';
import { cn } from '@/lib/utils';

interface PageLayoutProps extends React.HTMLAttributes<HTMLDivElement> {
  children: ReactNode;
}

interface PageHeaderProps extends React.HTMLAttributes<HTMLDivElement> {
  children: ReactNode;
}

interface PageTitleProps extends React.HTMLAttributes<HTMLHeadingElement> {
  children: ReactNode;
}

function PageLayout({ className, children, ...props }: PageLayoutProps) {
  return (
    <div className={cn("space-y-8", className)} {...props}>
      {children}
    </div>
  );
}

function PageHeader({ className, children, ...props }: PageHeaderProps) {
  return (
    <div className={cn("flex items-center justify-between", className)} {...props}>
      {children}
    </div>
  );
}

function PageTitle({ className, ...props }: PageTitleProps) {
    return <h1 className={cn("text-2xl font-bold tracking-tight", className)} {...props} />;
}

PageLayout.Header = PageHeader;
PageLayout.Title = PageTitle;

export { PageLayout };
