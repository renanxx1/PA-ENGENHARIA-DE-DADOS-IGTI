echo "#######################################"
echo "#INICIO DA INSTALAÇÃO DAS DEPENDENCIAS#"
echo "#######################################"

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

#Helm
sudo snap install helm --classic

#Telepresence
curl -s https://packagecloud.io/install/repositories/datawireio/telepresence/script.deb.sh | sudo bash
sudo apt install --no-install-recommends telepresence

#API
pip install websocket-client==0.56
pip install -U https://github.com/iqoptionapi/iqoptionapi/archive/refs/heads/master.zip

#Remove arquivos de instalação
sudo rm -rf kubectl kubectl.sha256

echo "#################################"
echo "#FIM INSTALAÇÃO DAS DEPENDENCIAS#"
echo "#################################"



#Cria cluster Kind
sudo kind create cluster --config kubernetes/kind/values.yaml

#Adiciona repositorio do kafka strimzi
sudo helm repo add strimzi https://strimzi.io/charts/

#Cria namespace kafka
sudo kubectl create namespace kafka

#Efetua deploy do kafka
sudo helm upgrade --install kafka strimzi/strimzi-kafka-operator --namespace kafka --version 0.29.0

#Cria o broker kafka
sudo kubectl apply -f kubernetes/kafka/broker/broker.yml -n kafka

#Adiciona repositorio do kafka-ui
sudo helm repo add kafka-ui https://provectus.github.io/kafka-ui

#Deploy do kafka-ui
sudo helm upgrade --install kafka-ui kafka-ui/kafka-ui --set envs.config.KAFKA_CLUSTERS_0_NAME=local --set envs.config.KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka-pa-kafka-bootstrap:9092 --namespace kafka


#Cria namespace
sudo kubectl create namespace spark

#Libera permissão na pasta kubernetes/spark
sudo chmod -R 777 kubernetes/spark/

#Adiciona o helm do spark
sudo helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

#Cria volumes
sudo kubectl apply -f kubernetes/kind/pvc.yaml -n spark

#Instala o helm do spark-operator
sudo helm upgrade --install spark-operator spark-operator/spark-operator --set webhook.enable=true --set sparkJobNamespace=spark -n spark


#Aguarda serviços iniciarem
sudo kubectl wait kafka/kafka-pa --for=condition=Ready --timeout=600s -n kafka 

#Pega o IP do cluster
echo ""
echo "############################################################################################"
kubectl get svc kafka-pa-kafka-bootstrap -n kafka -o jsonpath='KAFKA BROKER BOOTSTRAP IP/PORT: {"\n"}{.spec.clusterIP}:{.spec.ports[1].port}{"\n"}'
echo "############################################################################################"
echo ""

echo "############################################################################################"
kubectl get svc kafka-ui -n kafka -o jsonpath='KAFKA UI DISPONIVEL NO ENDEREÇO: {"\n"}http://{.spec.clusterIP}:{.spec.ports[0].port}{"\n"}'    
echo "############################################################################################"
echo ""

#Remove arquivos temporatios
sudo rm -rf get-docker.sh telepresence.log

#Executa o telepresence para ter acesso ao cluster
telepresence --context kind-kind --namespace kafka

