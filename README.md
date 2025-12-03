<h1 align="center">MeshCore – Nodi “Osservatori” &amp; meshcore-to-maps</h1>

<p align="center">
Strumento per la raccolta e l'analisi dei pacchetti MeshCore tramite MQTT e visualizzazione su mappa geografica.
</p>

---

<h2>Cos'è un nodo "Osservatore"?</h2>

I nodi MeshCore denominati <strong>“osservatori”</strong> sono nodi MeshCore connessi tramite MQTT che segnalano a questo servizio i pacchetti che ricevono.

Raccogliendo dati da più Osservatori in diverse sedi, è possibile:

- analizzare l'affidabilità della rete mesh;
- studiare il routing dei pacchetti;
- valutare la copertura radio da diverse prospettive.

Gli Osservatori (nodi MeshCore) possono essere di tipo:

- <strong>Ripetitori</strong>
- <strong>Server Room</strong>
- <strong>Companion (client)</strong>

Gli “osservatori” possono condividere la quantità di dati che desiderano.  
Ad esempio, potrebbero voler condividere solo i <em>pacchetti pubblicitari</em> per contribuire a una mappa automatizzata dei nodi Internet, ma non il contenuto di altri pacchetti.

---

<h2>I nodi osservatori creano dipendenza da Internet?</h2>

<strong>No.</strong> I nodi “osservatori” di MeshCore <strong>non</strong> creano una dipendenza dalla rete Internet.

La registrazione e l'analisi dei pacchetti servono a:

- comprendere le prestazioni e l'affidabilità della rete mesh;
- capire come avviene il routing dei pacchetti;
- valutare quali collegamenti sono validi e quali potrebbero richiedere miglioramenti;
- decidere se è necessario adottare un filtro SAV per limitare interferenze locali;
- valutare l’installazione di ripetitori aggiuntivi nelle aree con copertura critica;
- trovare percorsi (path) e rotte (routing) affidabili tra i diversi nodi della rete mesh.

Queste attività <strong>non</strong> servono a:

- estendere la portata della rete tramite Internet;
- collegare tra loro mesh o aree geografiche diverse;
- determinare se un messaggio viene ricevuto o meno.

Se un nodo osservatore in uplink perde la connessione Internet, significa solamente che questo strumento perde la possibilità di comprendere <em>la sua prospettiva</em> sulla rete. La rete MeshCore continua a funzionare in maniera autonoma.

MeshCore è progettato per essere una rete decentralizzata che <strong>non richiede Internet</strong> per funzionare. È un punto fondamentale che la distingue da molti altri strumenti.

MQTT è spesso associato al bridging tra reti differenti (come nel caso di Meshtastic), ma questo <strong>non</strong> è un caso d'uso previsto per questo progetto.

Qualsiasi utilizzo dei dati o delle API per:

- collegare diversi nodi/mesh/aree geografiche tramite Internet, oppure
- creare ponti permanenti tra reti mesh distinte,

esula dallo scopo di questo strumento e <strong>non sarà supportato</strong>.

Se viene rilevato un utilizzo dei dati o delle API in tal senso, tale funzionalità potrà essere disabilitata.

---

<h2>Come attivare e aggiungere un nodo osservatore alla mappa</h2>

Mappa geografica nazionale:  
👉 <a href="https://nodi.meshcoreitalia.it" target="_blank">https://nodi.meshcoreitalia.it</a>

Di seguito i passaggi per attivare un nodo osservatore e collegarlo a <code>meshcore-to-maps</code>.

---

<h3>Passaggio 1 – Scaricare il firmware per la registrazione dei pacchetti</h3>

Per il momento è disponibile il download diretto dei seguenti firmware:

👉 <a href="https://nextcloud.delink.it/index.php/s/qCHBFF7migwsx6W" target="_blank">Firmware MeshCore – Nextcloud</a>

Firmware disponibili:

- <code>Heltec_v3_repeater ver. 1.11.0</code>
- <code>Heltec_v4_repeater ver. 1.11.0</code>

Per firmware personalizzati su altri dispositivi, inviare una richiesta a:  
📧 <a href="mailto:info@meshcoreitalia.it">info@meshcoreitalia.it</a>  
specificando i dettagli del proprio device/hardware (marca, modello, versione, ecc.).

---

<h3>Passaggio 2 – Aggiornare / flashare il firmware</h3>

Il flashing del firmware può essere eseguito collegandosi al sito ufficiale:

👉 <a href="https://flasher.meshcore.dev" target="_blank">https://flasher.meshcore.dev</a>

1. Scorrere in fondo alla pagina e cliccare su <strong>“Custom Firmware”</strong>.
2. Nella finestra che si apre, selezionare il file del firmware scaricato relativo al proprio device.
3. Eseguire il flashing seguendo le indicazioni del sito.

Dopo il flashing:

- Nella stessa pagina, in alto a destra, cliccare su <strong>“Repeater Setup”</strong> per la programmazione del nodo
  <strong>oppure</strong>
- Usare la classica connessione Bluetooth supportata dall’app MeshCore (mobile).

---

<h3>Passaggio 3 – Installazione e configurazione di meshcore-to-maps</h3>

<h4>3.1 – Installare i pacchetti di base</h4>
<pre><code>sudo apt update
sudo apt install -y git python3 python3-venv python3-pip
</code></pre>

<h4>3.2 – Clonare il repository</h4>
<pre><code>cd /home/meshcore
git clone https://github.com/xpinguinx/meshcore-to-maps.git
cd meshcore-to-maps
</code></pre>

<h4>3.3 – Creare e attivare l’ambiente Python (virtualenv)</h4>
<pre><code>cd ~/meshcore-to-maps

python3 -m venv venv
source venv/bin/activate
</code></pre>

<h4>3.4 – Installare le dipendenze Python</h4>
<pre><code>pip install --upgrade pip
pip install -r requirements.txt
</code></pre>

<h4>3.5 – Configurare le variabili ambiente (.env.local)</h4>
<pre><code>nano .env.local
</code></pre>
<p>
Compilare i parametri richiesti in base alle proprie esigenze (vedi il
<em>Passaggio 4</em>).
</p>

<hr>

<h3>Passaggio 4 – Configurazione del file <code>.env.local</code></h3>

<p>Il parametro principale da verificare è:</p>

<pre><code>MCTOMQTT_SERIAL_PORTS=
</code></pre>

<p>
Questa variabile indica la porta seriale dove è collegato il repeater MeshCore.
</p>

<p>Esempi:</p>

<pre><code>MCTOMQTT_SERIAL_PORTS=/dev/ttyUSB0
# oppure
MCTOMQTT_SERIAL_PORTS=/dev/ttyACM0
</code></pre>

<p>
<strong>Tutti gli altri parametri</strong> non dovrebbero essere modificati,
pena il mancato funzionamento del servizio.
</p>

<h4>4.1 – Come identificare la porta seriale corretta</h4>

<p>Alcuni comandi utili in Linux:</p>

<p><strong>Lista dispositivi USB:</strong></p>
<pre><code>lsusb
</code></pre>

<p><strong>Porte seriali USB:</strong></p>
<pre><code>ls /dev/ttyUSB* /dev/ttyACM*
</code></pre>

<p><strong>Nomi stabili e leggibili:</strong></p>
<pre><code>ls -l /dev/serial/by-id
ls -l /dev/serial/by-path
</code></pre>

<p><strong>Capire quale porta ha impegnato il device</strong>
(subito dopo averlo collegato):</p>
<pre><code>dmesg | tail -n 20
</code></pre>

<hr>

<h3>Passaggio 5 – Avvio e verifica di meshcore-mctomqtt</h3>

<h4>5.1 – Test manuale del programma</h4>

<p>Con il virtualenv attivo e nella cartella del progetto:</p>

<pre><code>python mctomqtt.py
</code></pre>

<p>oppure:</p>

<pre><code>chmod +x run_mctomqtt.sh
./run_mctomqtt.sh
</code></pre>

<p>
Se il programma parte senza errori, puoi procedere con la creazione del servizio
<code>systemd</code>.
</p>

<h4>5.2 – Copiare il service file</h4>
<pre><code>cd ~/meshcore-to-maps
sudo cp meshcore-mctomqtt.service /etc/systemd/system/
</code></pre>

<h4>5.3 – Modificare il service file (se necessario)</h4>
<pre><code>sudo nano /etc/systemd/system/meshcore-mctomqtt.service
</code></pre>

<p>Controllare in particolare:</p>

<ul>
  <li>
    la riga <code>User=</code> – deve essere l’utente che esegue il programma,
    ad esempio:
    <pre><code>User=meshcore
</code></pre>
  </li>
  <li>
    la riga <code>ExecStart=</code> – deve puntare al percorso corretto,
    ad esempio:
    <pre><code>ExecStart=/home/meshcore/meshcore-to-maps/run_mctomqtt.sh
</code></pre>
  </li>
</ul>

<p>Salvare ed uscire dall’editor.</p>

<h4>5.4 – Abilitare e avviare il servizio</h4>

<pre><code>sudo systemctl daemon-reload
sudo systemctl enable meshcore-mctomqtt.service
sudo systemctl start meshcore-mctomqtt.service
sudo systemctl status meshcore-mctomqtt.service
</code></pre>

<p>Per la visualizzazione dei log in tempo reale:</p>

<pre><code>sudo journalctl -u meshcore-mctomqtt.service -f
</code></pre>

<hr>

<h3>Passaggio 6 – Rappresentazione grafica/geografica dei nodi</h3>

<p>
Se i passaggi precedenti sono stati eseguiti correttamente, dopo pochi secondi
il nodo osservatore sarà pubblicato e visibile nella mappa geografica nazionale:
</p>

<p>
👉 <a href="https://nodi.meshcoreitalia.it" target="_blank">
https://nodi.meshcoreitalia.it
</a>
</p>

<p>Ricorda che:</p>

<ul>
  <li>il nodo osservatore deve essere correttamente configurato e online;</li>
  <li>
    è necessario eseguire il comando
    <strong><code>advert</code></strong> per annunciare la sua presenza
    alla rete mesh.
  </li>
</ul>

<p>
Lo stesso comando <code>advert</code> dovrà essere inviato su qualsiasi altro nodo al fine di "annunciare" se stetto sull'intera rete meshlora.
</p>
