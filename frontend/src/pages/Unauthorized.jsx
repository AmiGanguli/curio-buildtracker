import { ShieldAlert, LogOut } from 'lucide-react';
import { useAuthenticator } from '@aws-amplify/ui-react';

function Unauthorized() {
    const { signOut } = useAuthenticator((context) => [context.user]);

    return (
        <div className="min-h-screen bg-gray-50 flex flex-col items-center justify-center p-4">
            <div className="bg-white p-8 rounded-2xl shadow-sm border border-gray-200 max-w-md w-full text-center">
                <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-6">
                    <ShieldAlert className="text-red-600" size={32} />
                </div>

                <h1 className="text-2xl font-bold text-gray-900 mb-2">Access Denied</h1>
                <p className="text-gray-500 mb-8">
                    You do not have permission to access the admin interface.
                    <br />
                    Please contact your administrator for access.
                </p>

                <button
                    onClick={signOut}
                    className="w-full flex items-center justify-center gap-2 bg-white border border-gray-300 text-gray-700 font-medium py-2.5 px-4 rounded-lg hover:bg-gray-50 transition-colors"
                >
                    <LogOut size={18} />
                    Sign Out
                </button>
            </div>
        </div>
    );
}

export default Unauthorized;
