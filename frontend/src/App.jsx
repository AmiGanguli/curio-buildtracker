import { useEffect, useState } from 'react';
import { Amplify } from 'aws-amplify';
import { Authenticator, useAuthenticator } from '@aws-amplify/ui-react';
import { fetchAuthSession } from 'aws-amplify/auth';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import '@aws-amplify/ui-react/styles.css';

import config from './amplify-config';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import Inputs from './pages/Inputs';
import Unauthorized from './pages/Unauthorized';

Amplify.configure(config);

function AuthenticatedApp() {
  const { user } = useAuthenticator((context) => [context.user]);
  const [isAuthorized, setIsAuthorized] = useState(false);
  const [checking, setChecking] = useState(true);

  useEffect(() => {
    const checkGroups = async () => {
      try {
        const session = await fetchAuthSession();
        const groups = session.tokens?.accessToken.payload['cognito:groups'] || [];

        // Deny if no groups OR only 'public' group
        // (i.e., we need at least one group that IS NOT 'public')
        const hasPrivilegedGroup = groups.some(g => g !== 'public');

        setIsAuthorized(hasPrivilegedGroup);
      } catch (e) {
        console.error("Failed to check groups", e);
        setIsAuthorized(false);
      } finally {
        setChecking(false);
      }
    };

    if (user) {
      checkGroups();
    }
  }, [user]);

  if (checking) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (!isAuthorized) {
    return <Unauthorized />;
  }

  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Dashboard />} />
          <Route path="/inputs" element={<Inputs />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

function App() {
  return (
    <Authenticator>
      <AuthenticatedApp />
    </Authenticator>
  );
}

export default App;
