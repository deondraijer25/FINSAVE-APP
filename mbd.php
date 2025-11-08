<?php

require_once(dirname(__FILE__).'/aqopi2finsave.php');
require_once(dirname(__FILE__).'/api.php');
require_once(dirname(__FILE__).'/aqopidata.php');

class Mbd extends Api {

    private $userHash;
    private $isPartner;
    
    private function __construct($connection, $guid, $userHash, $isPartner) {
        parent::__construct($connection, $guid);
        $this->userHash = $userHash;
        $this->isPartner = $isPartner;
    }
    
    private function getBelastingjaar($mbdData) {
        $result = $mbdData['belastingjaar'];
        if (is_null($result)) {
            throw new Exception('Exception in Mbd.getBelastingjaar: data.mbdVoorafIngevuldeGegevens.belastingjaar is null');     
        }
        return $result;
    }
    
    private function addBankrekeningen($batchId, $mbdData) {
        $data = $mbdData['bankrekeningen'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_bankrekening', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
      
    private function addEffectenrekeningen($batchId, $mbdData) {
        $data = $mbdData['effectenrekeningen'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_effectenrekening', 
            $data
        ); 
        while(mysqli_next_result($this->connection));
        return $result;
    }
        
    private function addHypotheken($batchId, $mbdData) {
        $data = $mbdData['hypotheken'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_hypotheek', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
        
    private function addInkomenLoondienst($batchId, $mbdData) {
        $data = $mbdData['inkomenLoondienst'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_inkomen_loondienst', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
        
    private function addOnroerendGoed($batchId, $mbdData) {
        $data = $mbdData['onroerendGoed'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_onroerend_goed', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
        
    private function addPersoonlijkeGegevens($batchId, $mbdData) {
        $data = $mbdData['persoonlijkeGegevens'];
        $result = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_persoon', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        if ($data) {
            $result->kinderen = $this->addKinderen($batchId, $data);
            $result->partner = $this->addPartner($batchId, $data);
        }
        return $result;
    }
               
    private function addKinderen($batchId, $pgData) {
        $data = $pgData['kinderen'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_kind', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
               
    private function addPartner($batchId, $pgData) {
        $data = $pgData['partner'];
        $result = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_partner', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
        
    private function addPremiesLijfrente($batchId, $mbdData) {
        $data = $mbdData['premiesLijfrente'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_premie_lijfrente', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
          
    private function addOverigeSchulden($batchId, $mbdData) {
        $data = $mbdData['overigeSchulden'];
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mbd_schuld', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
  
    private function pushData($mbdData) {
        $result = new stdClass();
        $result->batchId = AqopiData::newMbdHeader(
            $this->connection, 
            $this->userHash, 
            $this->guid, 
            0, 
            $this->isPartner, 
            $this->getBelastingjaar($mbdData)
        );        
        while(mysqli_next_result($this->connection));
        $batchId = $result->batchId->data;
        $this->logJson($batchId, $mbdData);
        
        $result->bankrekeningen = $this->addBankrekeningen($batchId, $mbdData);        
        $result->effectenrekeningen = $this->addEffectenrekeningen($batchId, $mbdData);
        $result->hypotheken = $this->addHypotheken($batchId, $mbdData);
        $result->inkomenLoondienst = $this->addInkomenLoondienst($batchId, $mbdData);
        $result->onroerendGoed = $this->addOnroerendGoed($batchId, $mbdData);
        $result->persoonlijkeGegevens = $this->addPersoonlijkeGegevens($batchId, $mbdData);
        $result->premiesLijfrente = $this->addPremiesLijfrente($batchId, $mbdData);  
        $result->overigeSchulden = $this->addOverigeSchulden($batchId, $mbdData);
        return $result;
    }
        
    private function validateData($data) {
        if (is_null($data)) {
            throw new Exception('Exception in Mbd.validateData: data is null');
        }
        if (is_null($data['mbdVoorafIngevuldeGegevens'])) {
            throw new Exception('Exception in Mbd.validateData: data.mbdVoorafIngevuldeGegevens is null');            
        }
    }
    
    private function pushResult($data) {
        $result = new stdClass();
        try { 
            $this->validateGuid(); // may raise exception
            $this->validateData($data); // may raise exceptions
            $result->data = $this->pushData($data['mbdVoorafIngevuldeGegevens']);
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->data = new stdClass();
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }
               
    static public function push($connection, $guid, $userHash, $isPartner, $data) {
        $mbd = new Mbd($connection, $guid, $userHash, $isPartner);
        $result = $mbd->pushResult($data);
        return $result;         
    }   
           
}