import os

# eventually automate this process. for now read from file
def get_local_auth_key(auth_key_path, auth_key_file):
    """Return authentification key from local text file inside keys/auth.txt"""
    path_to_file = os.path.join(auth_key_path, auth_key_file)
    
    try:
        with open(path_to_file, 'r') as f:
            key = f.readline()
            if isinstance(key, str):
                print('  Found auth key, returning')
                return key
            else:
                raise Exception('Error retrieving key from file. Check key exists.')
    except FileNotFoundError:
        print('Error finding auth key. Check key exists and check path')