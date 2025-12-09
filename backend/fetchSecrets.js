const { SecretsManagerClient, GetSecretValueCommand } = require("@aws-sdk/client-secrets-manager");

const client = new SecretsManagerClient({ region: "ap-southeast-1" });

module.exports = async () => {
  console.log("Secret Managers...");
  
  try {
    const secretName = "dev/contract-app/config";
    
    const command = new GetSecretValueCommand({ SecretId: secretName });
    const response = await client.send(command);
    
    if (response.SecretString) {
      console.log("Running");
      return JSON.parse(response.SecretString);
    }
    
    console.warn("Secret rỗng!");
    return {};
    
  } catch (error) {
    console.error("Secret Error", error.message);
    return {};
  }
};