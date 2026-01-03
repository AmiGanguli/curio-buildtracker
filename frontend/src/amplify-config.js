const config = {
    Auth: {
        Cognito: {
            userPoolId: import.meta.env.VITE_USER_POOL_ID,
            userPoolClientId: import.meta.env.VITE_USER_POOL_CLIENT_ID,
            identityPoolId: import.meta.env.VITE_IDENTITY_POOL_ID,
            loginWith: {
                email: true,
            }
        }
    },
    API: {
        REST: {
            "CurioApi": {
                endpoint: import.meta.env.VITE_API_URL,
                region: import.meta.env.VITE_REGION
            }
        }
    }
};

export default config;
