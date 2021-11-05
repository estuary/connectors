use super::errors::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, map, Value};
use std::collections::HashMap;

// The basic elastic search data types to represent data in Flow.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ESBasicType {
    Boolean,
    Date { format: String }, // refer to the comments of DateSpec in ../run.go for details.
    Double,
    Keyword { ignore_above: u16, dual_text: bool }, // refer to the comments of KeywordSpec in ../run.go for details.
    Long,
    Null,
    Text,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ESFieldType {
    Basic(ESBasicType),
    Object {
        properties: HashMap<String, ESFieldType>,
    },
}

// The type of a elastic search field is allowed to be overridden, so that
// more elastic-search-specific features, such as dual-text-keyword and date format,
// be specified.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ESTypeOverride {
    // The json pointer delimitated by '/'.
    pub pointer: String,
    // The overriding elastic search data type of the field.
    pub es_type: ESBasicType,
}

impl ESFieldType {
    pub fn render(&self) -> Value {
        match self {
            Self::Basic(b) => match b {
                ESBasicType::Boolean => self.render_basic(&"boolean"),
                ESBasicType::Date { format } => self.render_date(&format),
                ESBasicType::Double => self.render_basic(&"double"),
                ESBasicType::Keyword {
                    ignore_above,
                    dual_text,
                } => self.render_keyword(*ignore_above, *dual_text),
                ESBasicType::Long => self.render_basic(&"long"),
                ESBasicType::Null => Value::Null,
                ESBasicType::Text => self.render_basic(&"text"),
            },
            Self::Object { properties } => self.render_object(properties),
        }
    }

    fn render_basic(&self, type_name: &str) -> Value {
        return json!({ "type": type_name });
    }

    fn render_keyword(&self, ignore_above: u16, dual_text: bool) -> Value {
        let rendered_keyword = json!({
            "type": "keyword",
            "ignore_above": ignore_above
        });

        if dual_text {
            return json!({
                "type": "text",
                "fields": {"keyword": rendered_keyword}
            });
        }
        return rendered_keyword;
    }

    fn render_date(&self, format: &str) -> Value {
        return json!({
            "type": "date",
            "format": format
        });
    }

    fn render_object(&self, properties: &HashMap<String, ESFieldType>) -> Value {
        let mut field_map = map::Map::new();
        for (k, v) in properties.iter() {
            field_map.insert(k.clone(), v.render());
        }
        return json!({ "properties": field_map });
    }
}

impl ESFieldType {
    pub fn apply_type_override(mut self, es_override: &ESTypeOverride) -> Result<Self, Error> {
        let pointer = &es_override.pointer;
        if pointer.is_empty() {
            return Err(Error::override_error(POINTER_EMPTY, &pointer));
        }

        let mut cur_field = &mut self;
        let mut prop_itr = pointer
            .split('/')
            .skip(if pointer.starts_with('/') { 1 } else { 0 })
            .map(|fld| fld.to_string())
            .peekable();

        while let Some(prop) = prop_itr.next() {
            cur_field = match cur_field {
                Self::Object { properties } => {
                    if !properties.contains_key(&prop) {
                        return Err(Error::override_error(
                            &format!("{}: {}", POINTER_MISSING_FIELD, prop),
                            &pointer,
                        ));
                    } else if prop_itr.peek() == None {
                        properties.insert(prop, ESFieldType::Basic(es_override.es_type.clone()));
                        return Ok(self);
                    } else {
                        properties.get_mut(&prop).unwrap()
                    }
                }
                _ => return Err(Error::override_error(POINTER_WRONG_FIELD_TYPE, &pointer)),
            }
        }
        Ok(self)
    }
}
