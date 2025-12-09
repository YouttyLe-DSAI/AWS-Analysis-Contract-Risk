// src/server.ts
import app from './app';
import serverless from 'serverless-http';


if (process.env.AWS_LAMBDA_FUNCTION_VERSION) {
  console.log("🚀 Running on AWS Lambda");
}
else {
  const PORT = process.env.PORT || 3001;
  app.listen(PORT, () => {
    console.log(`🚀 Local Server running on http://localhost:${PORT}`);
  });
}
export const handler = serverless(app);