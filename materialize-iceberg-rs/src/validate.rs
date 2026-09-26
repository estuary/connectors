use anyhow::Result;
use proto_flow::{
    flow::{Projection, SerPolicy},
    materialize::{
        request::Validate,
        response::validated::{Binding, Constraint, ProjectionConstraint, constraint},
    },
};

use crate::configuration::Resource;

pub async fn do_validate(req: Validate) -> Result<Vec<Binding>> {
    req.bindings
        .iter()
        .map(|binding| {
            let res: Resource = serde_json::from_slice(&binding.resource_config_json)?;

            Ok(Binding {
                projection_constraints: binding
                    .collection
                    .as_ref()
                    .expect("binding must have collection spec")
                    .projections
                    .iter()
                    .map(|p| ProjectionConstraint {
                        field: p.field.clone(),
                        constraint: Some(constraint_for_projection(p)),
                    })
                    .collect(),
                resource_path: vec![res.table],
                delta_updates: true,
                case_insensitive_fields: false,
                ser_policy: Some(SerPolicy {
                    str_truncate_after: 1 << 16,
                    nested_obj_truncate_after: 1000,
                    array_truncate_after: 1000,
                }),
            })
        })
        .collect::<Result<Vec<Binding>>>()
}

fn constraint_for_projection(p: &Projection) -> Constraint {
    if p.is_primary_key {
        Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "Primary key locations should usually be materialized".to_string(),
            ..Default::default()
        }
    } else if p.ptr.is_empty() {
        Constraint {
            r#type: constraint::Type::FieldOptional.into(),
            reason: "The root document may be materialized".to_string(),
            ..Default::default()
        }
    } else if p.field == "flow_published_at" || !p.ptr.strip_prefix("/").unwrap().contains("/") {
        Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "Top-level locations should usually be materialized".to_string(),
            ..Default::default()
        }
    } else {
        Constraint {
            r#type: constraint::Type::FieldOptional.into(),
            reason: "This field may be materialized".to_string(),
            ..Default::default()
        }
    }
}
