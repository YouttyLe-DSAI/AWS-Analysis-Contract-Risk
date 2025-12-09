import { PostConfirmationTriggerEvent } from 'aws-lambda';
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(client);

export const handler = async (event: PostConfirmationTriggerEvent) => {
    if (event.triggerSource === "PostConfirmation_ConfirmSignUp") {
        const { sub, email } = event.request.userAttributes;
        const newUserItem = {
            user_id: sub,           
            email: email,
            created_at: new Date().toISOString(),
            plan: 'free',           
            credits_left: 5,        
        
        };

        const command = new PutCommand({
            TableName: process.env.DYNAMO_TABLE_USERS || 'Users', 
            Item: newUserItem,
            ConditionExpression: "attribute_not_exists(user_id)"
        });

        try {
            await docClient.send(command);
            console.log(`[Sync] Đã tạo user ${email} vào DynamoDB`);
        } catch (error) {
            console.error("Lỗi sync DynamoDB:", error);
        }
    }
    return event;
};