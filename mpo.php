<?php

require_once(dirname(__FILE__).'/aqopi2finsave.php');
require_once(dirname(__FILE__).'/api.php');
require_once(dirname(__FILE__).'/aqopidata.php');

class Mpo extends Api {

    private $userHash;
    private $isPartner;
    
    private function __construct($connection, $guid, $userHash, $isPartner) {
        parent::__construct($connection, $guid);
        $this->userHash = $userHash;
        $this->isPartner = $isPartner;
    }
    
    private function getGeslacht($mpoData) {
        $result = $mpoData['geslacht'];
        return $result;
    }
    
    private function getNaam($mpoData) {
        $result = $mpoData['naam'];
        return $result;
    }
    
    private function addPartnerPensioenRecht($batchId, $partnerPensioenId, $partnerPensioenRecht) {
        $result = new stdClass();        
        $result->pensioenRecht = AqopiData::addChildRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mpo_partner_pensioen_recht', 
            $partnerPensioenRecht,
            'partner_pensioen_rid',
            $partnerPensioenId
        );  
        while(mysqli_next_result($this->connection));
        
        $partnerPensioenRechtBedragen = $partnerPensioenRecht['bedragen'];
        if (!is_null($partnerPensioenRechtBedragen)) {
            if (    (!is_null($result->pensioenRecht->data)) 
                 && ($result->pensioenRecht->data->errorcode == 0) 
                 && (!is_null($result->pensioenRecht->data->aqopi))
            ) {
                $partnerPensioenRechtId = $result->pensioenRecht->data->aqopi;
                $result->pensioenRecht->bedragen = AqopiData::addChildRecordItem(
                    $this->connection, 
                    $this->userHash,
                    $batchId, 
                    't_mpo_partner_pensioen_recht_bedrag', 
                    $partnerPensioenRechtBedragen,
                    'partner_pensioen_recht_rid',
                    $partnerPensioenRechtId
                ); 
                while(mysqli_next_result($this->connection));
            }
        }
        return $result;
    }
    
    private function addPartnerPensioenRechten($batchId, $partnerPensioenId, $partnerPensioen) {        
        $data = $partnerPensioen['pensioenen'];      
        if (!is_null($data)) {
            if (is_array($data)) {
                $result = array();
                foreach ($data as $key => $partnerPensioenRecht) {
                    $result[$key] = $this->addPartnerPensioenRecht($batchId, $partnerPensioenId, $partnerPensioenRecht);
                }
            }
        } else {
            $result = 'No pensioenen data';
        }
        return $result;
    }
        
    private function addPartnerPensioenRechtenIndicatief($batchId, $partnerPensioenId, $partnerPensioen) {        
        $data = $partnerPensioen['pensioenenIndicatief'];      
        if (!is_null($data)) {
            if (is_array($data)) {
                $result = array();
                foreach ($data as $key => $partnerPensioenRecht) {
                    $result[$key] = $this->addPartnerPensioenRecht($batchId, $partnerPensioenId, $partnerPensioenRecht);
                }
            }
        } else {
            $result = 'No pensioenenIndicatief data';
        }
        return $result;
    }
    
    private function addPartnerPensioen($batchId, $partnerPensioen) {
        $result = new stdClass();        
        $result->partnerPensioen = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mpo_partner_pensioen', 
            $partnerPensioen
        );  
        while(mysqli_next_result($this->connection));
        
        if (    (!is_null($result->partnerPensioen->data)) 
             && ($result->partnerPensioen->data->errorcode == 0) 
             && (!is_null($result->partnerPensioen->data->aqopi))
        ) {
            $partnerPensioenId = $result->partnerPensioen->data->aqopi;
            $result->partnerPensioen->pensioenen = $this->addPartnerPensioenRechten($batchId, $partnerPensioenId, $partnerPensioen);
            $result->partnerPensioen->pensioenenIndicatief = $this->addPartnerPensioenRechtenIndicatief($batchId, $partnerPensioenId, $partnerPensioen);
        }
        return $result;
    }
        
    private function addPartnerPensioenDetails($batchId, $pdData) {
        $data = $pdData['partnerPensioenDetails'];      
        if (!is_null($data)) {
            if (is_array($data)) {
                $result = array();
                foreach ($data as $key => $partnerPensioen) {
                    $result[$key] = $this->addPartnerPensioen($batchId, $partnerPensioen);
                }
            }
        } else {
            $result = 'No partnerPensioenDetails data';
        }
        return $result;
    }
    
    private function addWezenPensioenRecht($batchId, $wezenPensioenId, $wezenPensioenRecht) {
        $result = new stdClass();        
        $result->pensioenRecht = AqopiData::addChildRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mpo_wezen_pensioen_recht', 
            $wezenPensioenRecht,
            'wezen_pensioen_rid',
            $wezenPensioenId
        );  
        while(mysqli_next_result($this->connection));
        
        $wezenPensioenRechtBedragen = $wezenPensioenRecht['bedragen'];
        if (!is_null($wezenPensioenRechtBedragen)) {
            if (    (!is_null($result->pensioenRecht->data)) 
                 && ($result->pensioenRecht->data->errorcode == 0) 
                 && (!is_null($result->pensioenRecht->data->aqopi))
            ) {
                $wezenPensioenRechtId = $result->pensioenRecht->data->aqopi;
                $result->pensioenRecht->bedragen = AqopiData::addChildRecordItem(
                    $this->connection, 
                    $this->userHash,
                    $batchId, 
                    't_mpo_wezen_pensioen_recht_bedrag', 
                    $wezenPensioenRechtBedragen,
                    'wezen_pensioen_recht_rid',
                    $wezenPensioenRechtId
                ); 
                while(mysqli_next_result($this->connection));
            }
        }
        return $result;
    }
    
    private function addWezenPensioenRechten($batchId, $wezenPensioenId, $wezenPensioen) {        
        $data = $wezenPensioen['pensioenen'];      
        if (!is_null($data)) {
            if (is_array($data)) {
                $result = array();
                foreach ($data as $key => $wezenPensioenRecht) {
                    $result[$key] = $this->addWezenPensioenRecht($batchId, $wezenPensioenId, $wezenPensioenRecht);
                }
            }
        } else {
            $result = 'No pensioenen data';
        }
        return $result;
    }
        
    private function addWezenPensioenRechtenIndicatief($batchId, $wezenPensioenId, $wezenPensioen) {        
        $data = $wezenPensioen['pensioenenIndicatief'];      
        if (!is_null($data)) {
            if (is_array($data)) {
                $result = array();
                foreach ($data as $key => $wezenPensioenRecht) {
                    $result[$key] = $this->addWezenPensioenRecht($batchId, $wezenPensioenId, $wezenPensioenRecht);
                }
            }
        } else {
            $result = 'No pensioenenIndicatief data';
        }
        return $result;
    }
    
    private function addWezenPensioen($batchId, $wezenPensioen) {
        $result = new stdClass();        
        $result->wezenPensioen = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mpo_wezen_pensioen', 
            $wezenPensioen
        );  
        while(mysqli_next_result($this->connection));
        
        if (    (!is_null($result->wezenPensioen->data)) 
             && ($result->wezenPensioen->data->errorcode == 0) 
             && (!is_null($result->wezenPensioen->data->aqopi))
        ) {
            $wezenPensioenId = $result->wezenPensioen->data->aqopi;
            $result->wezenPensioen->pensioenen = $this->addWezenPensioenRechten($batchId, $wezenPensioenId, $wezenPensioen);
            $result->wezenPensioen->pensioenenIndicatief = $this->addWezenPensioenRechtenIndicatief($batchId, $wezenPensioenId, $wezenPensioen);
        }
        return $result;
    }
        
    private function addWezenPensioenDetails($batchId, $pdData) {
        $data = $pdData['wezenPensioenDetails'];      
        if (!is_null($data)) {
            if (is_array($data)) {
                $result = array();
                foreach ($data as $key => $wezenPensioen) {
                    $result[$key] = $this->addWezenPensioen($batchId, $wezenPensioen);
                }
            }
        } else {
            $result = 'No wezenPensioenDetails data';
        }
        return $result;
    }
        
    private function addOuderdomPensioenRechten($batchId, $ouderdomPensioenId, $ouderdomPensioen) {        
        $data = $ouderdomPensioen['pensioenen'];      
        if (!is_null($data)) {
            $result = AqopiData::addChildRecordList(
                $this->connection, 
                $this->userHash,
                $batchId, 
                't_mpo_ouderdom_pensioen_recht', 
                $data,
                'ouderdom_pensioen_rid',
                $ouderdomPensioenId
            ); 
            while(mysqli_next_result($this->connection));
        } else {
            $result = 'No pensioenen data';
        }
        return $result;
    }
        
    private function addOuderdomPensioenRechtenIndicatief($batchId, $ouderdomPensioenId, $ouderdomPensioen) {        
        $data = $ouderdomPensioen['pensioenenIndicatief'];      
        if (!is_null($data)) {
            $result = AqopiData::addChildRecordList(
                $this->connection, 
                $this->userHash,
                $batchId, 
                't_mpo_ouderdom_pensioen_recht', 
                $data,
                'ouderdom_pensioen_rid',
                $ouderdomPensioenId
            ); 
            while(mysqli_next_result($this->connection));
        } else {
            $result = 'No pensioenenIndicatief data';
        }
        return $result;
    }
    
    private function addOuderdomPensioen($batchId, $ouderdomPensioen) {
        $result = new stdClass();        
        $result->ouderdomPensioen = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_mpo_ouderdom_pensioen', 
            $ouderdomPensioen
        );  
        while(mysqli_next_result($this->connection));
        
        if (    (!is_null($result->ouderdomPensioen->data)) 
             && ($result->ouderdomPensioen->data->errorcode == 0) 
             && (!is_null($result->ouderdomPensioen->data->aqopi))
        ) {
            $ouderdomPensioenId = $result->ouderdomPensioen->data->aqopi;
            $result->ouderdomPensioen->pensioenen = $this->addOuderdomPensioenRechten($batchId, $ouderdomPensioenId, $ouderdomPensioen);
            $result->ouderdomPensioen->pensioenenIndicatief = $this->addOuderdomPensioenRechtenIndicatief($batchId, $ouderdomPensioenId, $ouderdomPensioen);
        }
        return $result;
    }
        
    private function addOuderdomPensioenDetails($batchId, $pdData) {
        $data = $pdData['ouderdomPensioenDetails'];      
        if (!is_null($data)) {
            if (is_array($data)) {
                $result = array();
                foreach ($data as $key => $ouderdomPensioen) {
                    $result[$key] = $this->addOuderdomPensioen($batchId, $ouderdomPensioen);
                }
            }
        } else {
            $result = 'No ouderdomPensioenDetails data';
        }
        return $result;
    }
  
    private function pushData($mpoData) {
        $result = new stdClass();
        $result->batchId = AqopiData::newMpoHeader(
            $this->connection, 
            $this->userHash, 
            $this->guid, 
            0, 
            $this->isPartner,
            $this->getGeslacht($mpoData),
            $this->getNaam($mpoData)
        );        
        while(mysqli_next_result($this->connection));
        $batchId = $result->batchId->data;     
        $this->logJson($batchId, $mpoData);  
        
        $data = $mpoData['pensioenDetails'];
        if (!is_null($data)) {
            $result->partnerPensioenDetails = $this->addPartnerPensioenDetails($batchId, $data);        
            $result->wezenPensioenDetails = $this->addWezenPensioenDetails($batchId, $data);         
            $result->ouderdomPensioenDetails = $this->addOuderdomPensioenDetails($batchId, $data);             
        } else {
            $result->partnerPensioenDetails = 'No pensioenDetails data';        
            $result->wezenPensioenDetails = 'No pensioenDetails data'; 
            $result->ouderdomPensioenDetails = 'No pensioenDetails data';  
        }
        $data = $mpoData['pensioenTotalen'];
        if (!is_null($data)) {
            $result->samenvatting = AqopiData::addRecordList(
                $this->connection, 
                $this->userHash,
                $batchId, 
                't_mpo_samenvatting', 
                $data
            );  
            while(mysqli_next_result($this->connection));
        } else {
            $result->samenvatting = 'No pensioenTotalen data'; 
        }
        return $result;
    }
        
    private function validateData($data) {
        if (is_null($data)) {
            throw new Exception('Exception in Mpo.validateData: data is null');
        }
        if (is_null($data['mpoPensioengegevens'])) {
            throw new Exception('Exception in Mpo.validateData: data.mpoPensioengegevens is null');            
        }
    }
    
    private function pushResult($data) {
        $result = new stdClass();
        try { 
            $this->validateGuid(); // may raise exception
            $this->validateData($data); // may raise exceptions
            $result->data = $this->pushData($data['mpoPensioengegevens']);
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
        $mpo = new Mpo($connection, $guid, $userHash, $isPartner);
        $result = $mpo->pushResult($data);
        return $result;         
    }   
           
}