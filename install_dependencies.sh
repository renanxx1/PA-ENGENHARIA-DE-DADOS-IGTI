echo "###################################################"
echo "#Instalação das dependencias para rodar o ambiente#"
echo "###################################################"

#Curl
sudo apt install curl -y

#Docker
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin -y

#Kind
sudo curl -L "https://kind.sigs.k8s.io/dl/v0.8.1/kind-$(uname)-amd64" -o /usr/local/bin/kind
sudo chmod +x /usr/local/bin/kind

#Kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
curl -LO "https://dl.k8s.io/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256" 
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

#Remove arquivos de instalação
sudo rm -rf kubectl kubectl.sha256

#Helm
sudo snap install helm --classic

#Remove arquivos de instalação
sudo rm -rf kubectl kubectl.sha256

#Telepresence
curl -s https://packagecloud.io/install/repositories/datawireio/telepresence/script.deb.sh | sudo bash
sudo apt install --no-install-recommends telepresence

#API
pip install websocket-client==0.56
pip install -U https://github.com/iqoptionapi/iqoptionapi/archive/refs/heads/master.zip