import { Routes, Route, Outlet, Navigate, useLocation } from "react-router-dom";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import Home from "./pages/Home";
import About from "./pages/About";
import Login from "./pages/Login";
import Signup from "./pages/Signup";
import ForgotPassword from "./pages/ForgotPassword";
import ResetPassword from "./pages/ResetPassword";
import { Dashboard } from "./pages/Dashboard";
import DeepAnalysis from "./pages/DeepAnalysis";
import { Templates } from "./pages/Templates";
import { Library } from "./pages/Library";
import { Inspections } from "./pages/Inspections";
import Account from "./pages/Account";
import { ContractEditor } from "./pages/ContractEditor";
import NotFound from "./pages/NotFound";
import Navigation from "./components/Navigation";
import { DashboardLayout } from "./components/DashboardLayout";
import VerifyEmail from "./pages/VerifyEmail"; 

const App = () => {
  return (
    <AuthProvider>
      <AppRoutes />
    </AuthProvider>
  );
};

const PublicLayout = () => (
  <>
    <Navigation />
    <Outlet />
  </>
);

const ProtectedRoutes = () => {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div className="flex h-screen w-full items-center justify-center">
        <p>Loading...</p>
      </div>
    );
  }

  return user ? (
    <DashboardLayout>
      <Outlet />
    </DashboardLayout>
  ) : (
    <Navigate to="/login" state={{ from: location }} replace />
  );
};

const AppRoutes = () => {
  return (
    <Routes>
      {/* Public Routes */}
      <Route element={<PublicLayout />}>
        <Route path="/" element={<Home />} />
        <Route path="/about" element={<About />} />
      </Route>

      {/* Auth Routes */}
      <Route path="/login" element={<Login />} />
      <Route path="/signup" element={<Signup />} />
      
      {/* Verify Routes */}
      <Route path="/verify-email" element={<VerifyEmail />} />
      
      <Route path="/forgot-password" element={<ForgotPassword />} />
      <Route path="/reset-password" element={<ResetPassword />} />

      {/* Protected Dashboard Routes */}
      <Route element={<ProtectedRoutes />}>
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/deep-analysis" element={<DeepAnalysis />} />
        <Route path="/templates" element={<Templates />} />
        <Route path="/library" element={<Library />} />
        <Route path="/inspections" element={<Inspections />} />
        <Route path="/account" element={<Account />} />
        {/* New Editor Route */}
        <Route path="/templates/edit/:draftId" element={<ContractEditor />} />
        <Route path="/forgot-password" element={<ForgotPassword />} />
        <Route path="/reset-password" element={<ResetPassword />} />
      </Route>
      
      <Route path="*" element={<NotFound />} />
    </Routes>
  );
};

export default App;