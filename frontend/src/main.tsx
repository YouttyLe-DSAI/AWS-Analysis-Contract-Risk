import React from 'react';
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App.tsx";
import "./index.css";

import { Amplify } from 'aws-amplify';
import 'react-pdf/dist/Page/AnnotationLayer.css';
import 'react-pdf/dist/Page/TextLayer.css';

console.log("🔍 AWS Config Check:", {
  Region: import.meta.env.VITE_COGNITO_REGION,
  PoolID: import.meta.env.VITE_COGNITO_USER_POOL_ID,
  ClientID: import.meta.env.VITE_COGNITO_CLIENT_ID
});

Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId: import.meta.env.VITE_COGNITO_USER_POOL_ID,
      userPoolClientId: import.meta.env.VITE_COGNITO_CLIENT_ID,
      loginWith: {
        email: true,
      }
    }
  }
});

createRoot(document.getElementById("root")!).render(
  <BrowserRouter>
    <App />
  </BrowserRouter>
);