export function getPublicEnv() {
    const gateway = process.env.NEXT_PUBLIC_GATEWAY_URL;
    const chatApi = process.env.NEXT_PUBLIC_CHAT_API_URL;
  
    return {
      gateway,
      chatApi,
      hasAll: Boolean(gateway && chatApi),
    };
  }