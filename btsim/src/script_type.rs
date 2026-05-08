use bitcoin::transaction::InputWeightPrediction;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScriptType {
    #[default]
    P2tr,
    P2wpkh,
    P2pkh,
}

impl ScriptType {
    pub(crate) fn input_weight_prediction(self) -> InputWeightPrediction {
        match self {
            ScriptType::P2tr => InputWeightPrediction::P2TR_KEY_DEFAULT_SIGHASH,
            ScriptType::P2wpkh => InputWeightPrediction::P2WPKH_MAX,
            ScriptType::P2pkh => InputWeightPrediction::P2PKH_COMPRESSED_MAX,
        }
    }

    pub(crate) fn input_weight_wu(self) -> u32 {
        const INPUT_BASE_WU_NO_SCRIPTSIG_LEN: u32 = (32 + 4 + 4) * 4;
        INPUT_BASE_WU_NO_SCRIPTSIG_LEN + self.input_weight_prediction().weight().to_wu() as u32
    }

    pub(crate) fn output_script_len(self) -> usize {
        match self {
            ScriptType::P2tr => 34,
            ScriptType::P2wpkh => 22,
            ScriptType::P2pkh => 25,
        }
    }

    pub(crate) fn output_weight_wu(self) -> u32 {
        let script_len = self.output_script_len();
        let output_len = 8 + 1 + script_len;
        (output_len as u32) * 4
    }

    pub(crate) fn is_segwit(self) -> bool {
        matches!(self, ScriptType::P2tr | ScriptType::P2wpkh)
    }
}
