import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

// Khởi tạo S3 Client
const s3Client = new S3Client({ 
    region: process.env.AWS_REGION 
});

// --- CẬP NHẬT LẠI TYPE ---
export type FolderType = 
    // --- NHÓM 1: LEGAL CORPUS ---
    | 'legal-corpus-raw'        
    | 'legal-corpus-embeddings' 

    // --- NHÓM 2: USER DATA ---
    | 'user-document'   
    | 'user-processed'          
    | 'user-generated'
    
    // --- NHÓM 3: GENERATED CONTRACTS ---
    | 'generated-monthly-user'
    | 'user-avatar'; 

export const uploadToS3 = async (
    fileBuffer: Buffer, 
    fileName: string, 
    mimeType: string,
    folderType: FolderType,
    userId?: string 
) => {
  let keyPrefix = '';
  
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');

  switch (folderType) {
      // === 1. LEGAL CORPUS ===
      case 'legal-corpus-raw':
          keyPrefix = 'legal-corpus/original-docs/';
          break;
      case 'legal-corpus-embeddings':
          keyPrefix = 'legal-corpus/embeddings/';
          break;

      // === 2. USER DATA ===
      case 'user-document':
          keyPrefix = userId ? `user-data/${userId}/documents/` : 'user-data/anonymous/documents/';
          break;
          
      case 'user-processed':
          keyPrefix = userId ? `user-data/${userId}/processed/` : 'user-data/anonymous/processed/';
          break;
          
      case 'user-generated':
          keyPrefix = userId ? `user-data/${userId}/generated-templates/` : 'user-data/anonymous/generated-templates/';
          break;
    
      case 'generated-monthly-user':
          if (!userId) throw new Error("Folder generated-monthly-user requires userId");
          keyPrefix = `user-data/${userId}/generated-templates/${year}/${month}/`;
          break;
          
      case 'user-avatar':
          keyPrefix = userId ? `user-data/${userId}/avatar/` : 'misc/avatar/';
          break;

      default:
          keyPrefix = 'misc/';
          break;
  }

 
  const key = `${keyPrefix}${Date.now()}_${fileName}`;

  await s3Client.send(new PutObjectCommand({
    Bucket: process.env.S3_BUCKET_RAW,
    Key: key,
    Body: fileBuffer,
    ContentType: mimeType,
  }));

  return key;
};

export const getDownloadUrl = async (s3Key: string) => {
    const command = new GetObjectCommand({
        Bucket: process.env.S3_BUCKET_RAW,
        Key: s3Key
    });
    return getSignedUrl(s3Client, command, { expiresIn: 3600 });
};