<?php

require_once(dirname(__FILE__).'/aqopi2finsave.php');
require_once(dirname(__FILE__).'/api.php');
require_once(dirname(__FILE__).'/aqopidata.php');

class Moh extends Api {

    private $userHash;
    private $isPartner;
    
    private function __construct($connection, $guid, $userHash, $isPartner) {
        parent::__construct($connection, $guid);
        $this->userHash = $userHash;
        $this->isPartner = $isPartner;
    }
        
    private function addFamiliegegevens($batchId, $mohData) {
        $data = $mohData['mohFamiliegegevens'];
        if (is_null($data)) {
            return 'No mohFamiliegegevens data';
        }
        $result = new stdClass();
        $result->burgerlijkeStaat = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_burgerlijke_staat', 
            $data['burgerlijkeStaat']
        );  
        while(mysqli_next_result($this->connection));
        if (!is_null($data['burgerlijkeStaat'])) {
            $result->burgerlijkeStaat->partner = AqopiData::addRecordItem(
                $this->connection, 
                $this->userHash,
                $batchId, 
                't_moh_partner', 
                $data['burgerlijkeStaat']['partner']
            ); 
            while(mysqli_next_result($this->connection));             
        }
        $result->kinderen = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_kind', 
            $data['kinderen']
        );  
        while(mysqli_next_result($this->connection));
        $result->ouders = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_ouder', 
            $data['ouders']
        ); 
        while(mysqli_next_result($this->connection));
        $result->voormaligePartners = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_voormalige_partner', 
            $data['voormaligePartners']
        ); 
        while(mysqli_next_result($this->connection));
        return $result;
    }
      
    private function addIdGegevens($batchId, $mohData) {
        $data = $mohData['mohIdGegevens'];
        if (is_null($data)) {
            return 'No mohIdGegevens data';
        }
        $result = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_paspoort', 
            $data['paspoort']
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
      
    private function addInkomengegevens($batchId, $mohData) {
        $data = $mohData['mohInkomengegevens'];
        if (is_null($data)) {
            return 'No mohInkomengegevens data';
        }
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_inkomen', 
            $data['inkomens']
        );  
        while(mysqli_next_result($this->connection));
        return $result;        
    }
      
    private function addKadastergegevens($batchId, $mohData) {
        $data = $mohData['mohKadastergegevens'];
        if (is_null($data)) {
            return 'No mohKadastergegevens data';
        }
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_onroerend_goed', 
            $data['onroerendGoedLijst']
        );  
        while(mysqli_next_result($this->connection));
        return $result;  
    }
      
    private function addNationaliteitgegevens($batchId, $mohData) {
        $data = $mohData['mohNationaliteitgegevens'];
        $result = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_nationaliteit', 
            $data
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
      
    private function addPersoonsGegevens($batchId, $mohData) {
        $data = $mohData['mohPersoonsgegevens'];
        if (is_null($data)) {
            return 'No mohPersoonsgegevens data';
        }
        $result = new stdClass();
        $result->adres = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_adres', 
            $data['adres']
        );  
        while(mysqli_next_result($this->connection));
        $result->algemeen = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_persoon', 
            $data['algemeen']
        );  
        while(mysqli_next_result($this->connection));
        $result->woongeschiedenis = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_woongeschiedenis', 
            $data['woongeschiedenis']
        );  
        while(mysqli_next_result($this->connection));        
        return $result;        
    }
      
    private function addWozGegevens($batchId, $mohData) {
        $data = $mohData['mohWozGegevens'];
        if (is_null($data)) {
            return 'No mohWozGegevens data';
        }
        $result = AqopiData::addRecordList(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_moh_woz', 
            $data['wozGegevens']
        );  
        while(mysqli_next_result($this->connection));
        return $result;        
    }
  
    private function pushData($mohData) {
        $result = new stdClass();
        $result->batchId = AqopiData::newMohHeader(
            $this->connection, 
            $this->userHash, 
            $this->guid, 
            0, 
            $this->isPartner
        );        
        while(mysqli_next_result($this->connection));
        $batchId = $result->batchId->data;
        $this->logJson($batchId, $mohData);
        
        $result->mohFamiliegegevens = $this->addFamiliegegevens($batchId, $mohData);        
        $result->mohIdGegevens = $this->addIdGegevens($batchId, $mohData);        
        $result->mohInkomengegevens = $this->addInkomengegevens($batchId, $mohData);         
        $result->mohKadastergegevens = $this->addKadastergegevens($batchId, $mohData);         
        $result->mohNationaliteitgegevens = $this->addNationaliteitgegevens($batchId, $mohData);               
        $result->mohPersoonsGegevens = $this->addPersoonsGegevens($batchId, $mohData);  
        $result->mohWozGegevens = $this->addWozGegevens($batchId, $mohData);    
        return $result;
    }
        
    private function validateData($data) {
        if (is_null($data)) {
            throw new Exception('Exception in Moh.validateData: data is null');
        }
    }
    
    private function pushResult($data) {
        $result = new stdClass();
        try { 
            $this->validateGuid(); // may raise exception
            $this->validateData($data); // may raise exceptions
            $result->data = $this->pushData($data);
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
        $moh = new Moh($connection, $guid, $userHash, $isPartner);
        $result = $moh->pushResult($data);
        return $result;         
    }   
           
}