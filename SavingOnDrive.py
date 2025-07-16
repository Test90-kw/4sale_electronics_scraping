import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta

class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict  # Google service account credentials as a Python dict
        self.scopes = ['https://www.googleapis.com/auth/drive']  # Full access to Drive
        self.service = None  # Will hold authenticated Google Drive service
        self.parent_folder_id = '1NqWSVrV95XdnCbZ5MCqVR-4O2JxCF3Up'  # ID of the parent folder on Drive

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            print("Authenticating with Google Drive...")
            # Authenticate using service account credentials and scopes
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build the Drive service client
            self.service = build('drive', 'v3', credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            # Catch any authentication issues
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name):
        """Get folder ID by name within the parent folder."""
        try:
            # Build a query to search for the folder by name under the specified parent folder
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            # Execute the query to find matching folders
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            # Extract the list of folders
            files = results.get('files', [])
            if files:
                # Return the ID of the first matching folder
                print(f"Folder '{folder_name}' found with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                print(f"Folder '{folder_name}' does not exist.")
                return None
        except Exception as e:
            # Handle API or query errors
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name):
        """Create a new folder in the parent folder."""
        try:
            print(f"Creating folder '{folder_name}'...")
            # Define the metadata for the new folder
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]
            }
            # Create the folder using the Drive API
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            # Raise error if folder creation fails
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            print(f"Uploading file: {file_name}")
            # Set file metadata including name and target folder
            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            # Wrap the file using MediaFileUpload for streaming upload
            media = MediaFileUpload(file_name, resumable=True)
            # Upload the file to Drive
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            # Catch file upload issues
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in a folder named after yesterday's date."""
        try:
            # Get yesterday's date as folder name (format: YYYY-MM-DD)
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            # Try to get the folder ID; create the folder if not found
            folder_id = self.get_folder_id(yesterday)
            if not folder_id:
                folder_id = self.create_folder(yesterday)
            
            # Upload each file to the designated folder
            for file_name in files:
                self.upload_file(file_name, folder_id)
            
            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            # Catch issues during file saving
            print(f"Error saving files: {e}")
            raise
