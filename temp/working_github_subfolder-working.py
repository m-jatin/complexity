import os
import pip

pip.main(['install', 'PyGithub'])
pip.main(['install', 'requests'])
from github import Github
import requests


def github_to_local(repo_link, token, local_path, subfolder_name):


    def download_subfolder(repo_link, token, local_path, subfolder_name):
      """
      Downloads all files recursively from a subfolder within a GitHub repository to a local path.
    
      Args:
        repo_link: The URL of the GitHub repository.
        token: A personal access token with sufficient permissions to access the repository.
        local_path: The local path where the downloaded files will be saved.
        subfolder_name: The name of the subfolder within the repository to download.
      """
    
      # Create a Github object using the provided token
      g = Github(token)
    
      # Extract the owner and repository name from the URL
      owner, repo_name = repo_link.split("/")[-2:]
    
      # Get the repository object
      repo = g.get_repo(f"{owner}/{repo_name}")
    
      def download_dir(contents, local_dir):
        # Create the local directory if it doesn't exist
        os.makedirs(os.path.join(local_path, local_dir), exist_ok=True)
    
        # Download each file or recurse into subfolders
        for content in contents:
          if content.type == "file":
            download_url = content.download_url
            local_file_path = os.path.join(local_path, local_dir, content.name)
    
            # Download the file and handle potential exceptions
            try:
              with open(local_file_path, "wb") as f:
                f.write(requests.get(download_url).content)
              print(f"Downloaded: {local_file_path}")
            except Exception as e:
              print(f"Error downloading {content.name}: {e}")
          elif content.type == "dir":
            # Recursively download subfolder content
            download_dir(repo.get_contents(content.path), os.path.join(local_dir, content.name))
    
      # Download the specified subfolder and its contents recursively
      download_dir(repo.get_contents(subfolder_name), subfolder_name)
    
    
    
    # Ensure the local path exists
    if not os.path.exists(local_path):
      raise ValueError(f"Local path '{local_path}' does not exist.")
    
    # Check for valid token
    try:
      Github(token)
    except Exception as e:
      raise ValueError(f"Invalid token provided: {e}")
    
    # Download the subfolder content recursively
    download_subfolder(repo_link, token, local_path, subfolder_name)

