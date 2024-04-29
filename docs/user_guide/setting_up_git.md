# Tracking your project on GitHub

After downloading the project template, you will need to setup a Github repository to track any changes you make to the files on your local machine. 

Follow these steps in order to do so 

### Create repo on GitHub:
* Log in to your GitHub account on the GitHub website
* Click on the "+" icon in the top right corner and select "New repository".
* Choose a name for your repository. This should ideally be the same name as the fodler on your local machine.
* Configure other settings such as visibility (public or private).
* Click on "Create repository" to finalize the creation.

### Copy URL
* Once the repository is created, copy the URL provided. (eg https://github.com/me/my-project-repo.git)
* This URL is required to establish a connection between your local repository and the GitHub repository.

### (In terminal) `git remote add origin {url}`
* Open your terminal or command prompt.
* Navigate to the directory of your existing project using the cd command.
* Use the `git remote add origin` command followed by the GitHub repository URL copied earlier to set the remote origin for your local repository.

### (In terminal) `git branch -m main`
* Rename the default branch from 'master' to 'main', to align with GitHub conventions.

### (In terminal) `git add .`
* Stage all project files 

### (In terminal) `git commit -m 'initial commit'`
* Commit your staged files 

### (In terminal) `git push -u origin main`
* Finally, use the `git push -u origin main command` to push your committed changes to the GitHub repository.
* This command establishes the 'main' branch on the remote repository and sets it as the default upstream branch for future pushes.

