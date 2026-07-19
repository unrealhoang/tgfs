use std::borrow::Cow;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse_macro_input, parse_str, spanned::Spanned, Attribute, Data, DataStruct, DeriveInput,
    Error, ExprPath, Field, Fields, LitStr, Result, Type,
};

/// Calls the fallible entry point and writes any errors to the tokenstream.
#[proc_macro_derive(FromRow, attributes(from_row))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);

    try_derive_from_row(derive_input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Fallible entry point for generating a `FromRow` implementation
fn try_derive_from_row(input: DeriveInput) -> Result<TokenStream2> {
    let from_row_derive = DeriveFromRow::parse(input)?;

    Ok(from_row_derive.generate())
}

/// Main struct for deriving `FromRow` for a struct.
struct DeriveFromRow {
    ident: syn::Ident,
    generics: syn::Generics,
    data: Vec<FromRowField>,
}

impl DeriveFromRow {
    fn parse(input: DeriveInput) -> Result<Self> {
        let DeriveInput {
            ident,
            generics,
            data:
                Data::Struct(DataStruct {
                    fields: Fields::Named(fields),
                    ..
                }),
            ..
        } = input
        else {
            return Err(Error::new(
                input.span(),
                "expected struct with named fields",
            ));
        };

        let mut data = Vec::new();

        for field in fields.named {
            data.push(FromRowField::parse(field)?);
        }

        Ok(Self {
            ident,
            generics,
            data,
        })
    }

    fn predicates(&self) -> Vec<TokenStream2> {
        let mut predicates = Vec::new();

        for field in &self.data {
            field.add_predicates(&mut predicates);
        }

        predicates
    }

    /// Generate the `FromRow` implementation.
    fn generate(self) -> TokenStream2 {
        let ident = &self.ident;

        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let original_predicates = where_clause.map(|w| &w.predicates).into_iter();
        let predicates = self.predicates();

        let is_all_null_fields = self.data.iter().filter_map(|f| f.generate_is_all_null());

        let try_from_row_fields = self
            .data
            .iter()
            .map(|field| field.generate_try_from_row(false));
        let try_from_row_prefixed_fields = self
            .data
            .iter()
            .map(|field| field.generate_try_from_row(true));

        quote! {
            impl #impl_generics rusqlite_from_row::FromRow for #ident #ty_generics where #(#original_predicates),* #(#predicates),* {
                fn try_from_row(
                    row: &rusqlite_from_row::rusqlite::Row,
                ) -> std::result::Result<Self, rusqlite_from_row::rusqlite::Error> {
                    Ok(Self {
                        #(#try_from_row_fields),*
                    })
                }

                fn try_from_row_prefixed(
                    row: &rusqlite_from_row::rusqlite::Row,
                    prefix: Option<&str>
                ) -> std::result::Result<Self, rusqlite_from_row::rusqlite::Error> {
                    Ok(Self {
                        #(#try_from_row_prefixed_fields),*
                    })
                }

                fn is_all_null(
                    row: &rusqlite_from_row::rusqlite::Row,
                    prefix: Option<&str>
                ) -> std::result::Result<bool, rusqlite_from_row::rusqlite::Error> {
                    Ok(#(#is_all_null_fields)&&*)
                }
            }
        }
    }
}

/// A single field inside of a struct that derives `FromRow`
struct FromRowField {
    /// The identifier of this field.
    ident: syn::Ident,
    /// The type specified in this field.
    ty: syn::Type,
    attrs: FromRowAttrs,
}

impl FromRowField {
    pub fn parse(field: Field) -> Result<Self> {
        let attrs = FromRowAttrs::parse(field.attrs)?;

        Ok(Self {
            ident: field.ident.expect("should be named"),
            ty: field.ty,
            attrs,
        })
    }

    /// Returns a tokenstream of the type that should be returned from either
    /// `FromRow` (when using `flatten`) or `FromSql`.
    fn target_ty(&self) -> Option<&Type> {
        match &self.attrs {
            FromRowAttrs::Field {
                convert: Some(Convert::From(ty) | Convert::TryFrom(ty)),
                ..
            } => Some(ty),
            FromRowAttrs::Field {
                convert: Some(Convert::FromFn(_)),
                ..
            } => None,
            _ => Some(&self.ty),
        }
    }

    /// Returns the name that maps to the actuall sql column
    /// By default this is the same as the rust field name but can be overwritten by `#[from_row(rename = "..")]`.
    fn column_name(&self) -> Cow<'_, str> {
        match &self.attrs {
            FromRowAttrs::Field {
                rename: Some(name), ..
            } => name.as_str().into(),
            _ => self.ident.to_string().into(),
        }
    }

    /// Pushes the needed where clause predicates for this field.
    ///
    /// By default this is `T: rusqlite::types::FromSql`,
    /// when using `flatten` it's: `T: rusqlite_from_row::FromRow`
    /// and when using either `from` or `try_from` attributes it additionally pushes this bound:
    /// `T: std::convert::From<R>`, where `T` is the type specified in the struct and `R` is the
    /// type specified in the `[try]_from` attribute.
    fn add_predicates(&self, predicates: &mut Vec<TokenStream2>) {
        match &self.attrs {
            FromRowAttrs::Field {
                default, convert, ..
            } => {
                let target_ty = self.target_ty();
                let ty = &self.ty;

                if let Some(target_ty) = target_ty {
                    predicates
                        .push(quote! (#target_ty: rusqlite_from_row::rusqlite::types::FromSql));

                    if *default {
                        predicates.push(quote! (#target_ty: ::std::default::Default));
                    }
                }

                match convert {
                    Some(Convert::From(target_ty)) => {
                        predicates.push(quote!(#target_ty: std::convert::From<#target_ty>))
                    }
                    Some(Convert::TryFrom(target_ty)) => {
                        let try_from = quote!(std::convert::TryFrom<#target_ty>);

                        predicates.push(quote!(#ty: #try_from));
                        predicates.push(quote!(rusqlite_from_row::rusqlite::Error: std::convert::From<<#ty as #try_from>::Error>));
                        predicates.push(quote!(<#ty as #try_from>::Error: std::fmt::Debug));
                    }
                    _ => {}
                }
            }
            FromRowAttrs::Flatten { default, .. } => {
                let ty = &self.ty;

                predicates.push(quote! (#ty: rusqlite_from_row::FromRow));

                if *default {
                    predicates.push(quote! (#ty: ::std::default::Default));
                }
            }
            FromRowAttrs::Skip => {
                let ty = &self.ty;

                predicates.push(quote! (#ty: ::std::default::Default));
            }
        }
    }

    fn generate_is_all_null(&self) -> Option<TokenStream2> {
        let is_all_null = match &self.attrs {
            FromRowAttrs::Flatten { prefix, .. } => {
                let ty = &self.ty;

                let prefix = match &prefix {
                    Some(Prefix::Value(prefix)) => {
                        quote!({
                            let nested_prefix = match prefix {
                                Some(prefix) => ::std::borrow::Cow::Owned(
                                    prefix.to_string() + #prefix
                                ),
                                None => ::std::borrow::Cow::Borrowed(#prefix),
                            };
                            Some(nested_prefix)
                        })
                    }
                    Some(Prefix::Field) => {
                        let ident_str = format!("{}_", self.ident);
                        quote!({
                            let nested_prefix = match prefix {
                                Some(prefix) => ::std::borrow::Cow::Owned(
                                    prefix.to_string() + #ident_str
                                ),
                                None => ::std::borrow::Cow::Borrowed(#ident_str),
                            };
                            Some(nested_prefix)
                        })
                    }
                    None => {
                        return Some(quote!(
                            <#ty as rusqlite_from_row::FromRow>::is_all_null(row, prefix)?
                        ));
                    }
                };

                quote!({
                    let nested_prefix = #prefix;
                    <#ty as rusqlite_from_row::FromRow>::is_all_null(
                        row,
                        nested_prefix.as_deref(),
                    )?
                })
            }
            FromRowAttrs::Field { .. } => {
                let column_name = self.column_name();

                quote! {
                    {
                        let column_name = match prefix {
                            Some(prefix) => ::std::borrow::Cow::Owned(
                                prefix.to_string() + #column_name
                            ),
                            None => ::std::borrow::Cow::Borrowed(#column_name),
                        };
                        rusqlite_from_row::rusqlite::Row::get_ref::<&str>(
                            row,
                            column_name.as_ref(),
                        )? == rusqlite_from_row::rusqlite::types::ValueRef::Null
                    }
                }
            }
            FromRowAttrs::Skip => return None,
        };

        Some(is_all_null)
    }

    /// Generate the line needed to retrieve this field from a row when calling `try_from_row`.
    fn generate_try_from_row(&self, prefixed: bool) -> TokenStream2 {
        let ident = &self.ident;
        let column_name = self.column_name();
        let field_ty = &self.ty;

        let base = match &self.attrs {
            FromRowAttrs::Flatten { prefix, default } => {
                let ty = &self.ty;
                let mapper_ty = if *default {
                    quote!(std::option::Option<#ty>)
                } else {
                    quote!(#ty)
                };

                let value = if prefixed {
                    let nested_prefix = match &prefix {
                        Some(Prefix::Value(prefix)) => quote!({
                            let nested_prefix = match prefix {
                                Some(parent_prefix) => ::std::borrow::Cow::Owned(
                                    parent_prefix.to_string() + #prefix
                                ),
                                None => ::std::borrow::Cow::Borrowed(#prefix),
                            };
                            Some(nested_prefix)
                        }),
                        Some(Prefix::Field) => {
                            let ident_str = format!("{}_", self.ident);
                            quote!({
                                let nested_prefix = match prefix {
                                    Some(parent_prefix) => ::std::borrow::Cow::Owned(
                                        parent_prefix.to_string() + #ident_str
                                    ),
                                    None => ::std::borrow::Cow::Borrowed(#ident_str),
                                };
                                Some(nested_prefix)
                            })
                        }
                        None => quote!(prefix.map(::std::borrow::Cow::Borrowed)),
                    };

                    quote!({
                        let nested_prefix: Option<::std::borrow::Cow<'_, str>> = #nested_prefix;
                        <#mapper_ty as rusqlite_from_row::FromRow>::try_from_row_prefixed(
                            row,
                            nested_prefix.as_deref(),
                        )?
                    })
                } else {
                    match &prefix {
                        Some(Prefix::Value(prefix)) => quote!(
                            <#mapper_ty as rusqlite_from_row::FromRow>::try_from_row_prefixed(
                                row,
                                Some(#prefix),
                            )?
                        ),
                        Some(Prefix::Field) => {
                            let ident_str = format!("{}_", self.ident);
                            quote!(
                                <#mapper_ty as rusqlite_from_row::FromRow>::try_from_row_prefixed(
                                    row,
                                    Some(#ident_str),
                                )?
                            )
                        }
                        None => quote!(
                            <#mapper_ty as rusqlite_from_row::FromRow>::try_from_row(row)?
                        ),
                    }
                };

                if *default {
                    quote! {
                        match #value {
                            Some(value) => value,
                            None => <#ty as ::std::default::Default>::default(),
                        }
                    }
                } else {
                    value
                }
            }
            FromRowAttrs::Field {
                convert, default, ..
            } => {
                let target_ty = self
                    .target_ty()
                    .cloned()
                    .unwrap_or_else(|| parse_str("_").unwrap());

                let base = if *default {
                    let get_value = if prefixed {
                        quote! {{
                            let column_name = match prefix {
                                Some(prefix) => ::std::borrow::Cow::Owned(
                                    prefix.to_string() + #column_name
                                ),
                                None => ::std::borrow::Cow::Borrowed(#column_name),
                            };
                            rusqlite_from_row::rusqlite::Row::get_ref::<&str>(
                                row,
                                column_name.as_ref(),
                            )?
                        }}
                    } else {
                        quote! {
                            rusqlite_from_row::rusqlite::Row::get_ref::<&str>(row, #column_name)?
                        }
                    };

                    quote! {
                        {
                            match #get_value {
                                rusqlite_from_row::rusqlite::types::ValueRef::Null => <#target_ty as ::std::default::Default>::default(),
                                value => <#target_ty as rusqlite_from_row::rusqlite::types::FromSql>::column_result(value)?,
                            }
                        }
                    }
                } else if prefixed {
                    quote!({
                        let column_name = match prefix {
                            Some(prefix) => ::std::borrow::Cow::Owned(
                                prefix.to_string() + #column_name
                            ),
                            None => ::std::borrow::Cow::Borrowed(#column_name),
                        };
                        rusqlite_from_row::rusqlite::Row::get::<&str, #target_ty>(
                            row,
                            column_name.as_ref(),
                        )?
                    })
                } else {
                    quote!(rusqlite_from_row::rusqlite::Row::get::<&str, #target_ty>(
                        row,
                        #column_name,
                    )?)
                };

                match convert {
                    Some(Convert::From(_)) => {
                        quote!(<#field_ty as std::convert::From<#target_ty>>::from(#base))
                    }
                    Some(Convert::TryFrom(_)) => {
                        quote!(<#field_ty as std::convert::TryFrom<#target_ty>>::try_from(#base)?)
                    }
                    Some(Convert::FromFn(func)) => {
                        quote!(#func(#base))
                    }
                    _ => base,
                }
            }
            FromRowAttrs::Skip => {
                let ty = &self.ty;

                quote!(<#ty as std::default::Default>::default())
            }
        };

        quote!(#ident: #base)
    }
}

#[allow(clippy::large_enum_variant)]
enum FromRowAttrs {
    Flatten {
        prefix: Option<Prefix>,
        default: bool,
    },
    Field {
        rename: Option<String>,
        convert: Option<Convert>,
        default: bool,
    },
    Skip,
}

enum Convert {
    From(Type),
    TryFrom(Type),
    FromFn(ExprPath),
}

enum Prefix {
    Value(String),
    Field,
}

impl FromRowAttrs {
    fn parse(attrs: Vec<Attribute>) -> Result<FromRowAttrs> {
        let Some(span) = attrs.first().map(|attr| attr.span()) else {
            return Ok(Self::Field {
                rename: None,
                convert: None,
                default: false,
            });
        };

        let mut flatten = false;
        let mut prefix = None;
        let mut try_from = None;
        let mut from = None;
        let mut from_fn = None;
        let mut rename = None;
        let mut skip = false;
        let mut default = false;

        for attr in attrs {
            if !attr.meta.path().is_ident("from_row") {
                continue;
            }

            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("flatten") {
                    flatten = true;
                } else if meta.path.is_ident("prefix") {
                    let prefix_value = if let Ok(value) = meta.value() {
                        Prefix::Value(value.parse::<LitStr>()?.value())
                    } else {
                        Prefix::Field
                    };

                    prefix = Some(prefix_value);
                } else if meta.path.is_ident("try_from") {
                    let try_from_str: LitStr = meta.value()?.parse()?;
                    try_from = Some(parse_str(&try_from_str.value())?);
                } else if meta.path.is_ident("from") {
                    let from_str: LitStr = meta.value()?.parse()?;
                    from = Some(parse_str(&from_str.value())?);
                } else if meta.path.is_ident("from_fn") {
                    let from_fn_str: LitStr = meta.value()?.parse()?;
                    from_fn = Some(parse_str(&from_fn_str.value())?);
                } else if meta.path.is_ident("rename") {
                    let rename_str: LitStr = meta.value()?.parse()?;
                    rename = Some(rename_str.value());
                } else if meta.path.is_ident("skip") {
                    skip = true;
                } else if meta.path.is_ident("default") {
                    default = true;
                }

                Ok(())
            })?;
        }

        let attrs = if skip {
            let other_attrs = flatten
                || default
                || prefix.is_some()
                || try_from.is_some()
                || from_fn.is_some()
                || from.is_some()
                || rename.is_some();

            if other_attrs {
                return Err(Error::new(
                    span,
                    "can't combine `skip` with other attributes",
                ));
            }

            Self::Skip
        } else if flatten {
            if rename.is_some() || from.is_some() || try_from.is_some() || from_fn.is_some() {
                return Err(Error::new(
                    span,
                    "can't combine `skip` with other attributes",
                ));
            }

            Self::Flatten { default, prefix }
        } else {
            if prefix.is_some() {
                return Err(Error::new(
                    span,
                    "`prefix` attribute is only valid in combination with `flatten`",
                ));
            }

            let convert = match (try_from, from, from_fn) {
                (Some(try_from), None, None) => Some(Convert::TryFrom(try_from)),
                (None, Some(from), None) => Some(Convert::From(from)),
                (None, None, Some(from_fn)) => Some(Convert::FromFn(from_fn)),
                (None, None, None) => None,
                _ => {
                    return Err(Error::new(
                        span,
                        "can't combine `try_from`, `from` or `from_fn`",
                    ))
                }
            };

            Self::Field {
                rename,
                convert,
                default,
            }
        };

        Ok(attrs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unprefixed_mapper_does_not_build_column_names() {
        let input = syn::parse_quote! {
            struct Example {
                field: String,
                #[from_row(rename = "other_field")]
                other: i64,
            }
        };
        let generated = try_derive_from_row(input).unwrap().to_string();
        let direct_start = generated.find("fn try_from_row").unwrap();
        let prefixed_start = generated.find("fn try_from_row_prefixed").unwrap();
        let direct_mapper = &generated[direct_start..prefixed_start];

        assert!(!direct_mapper.contains("to_string"));
        assert!(!direct_mapper.contains("Cow"));
        assert!(direct_mapper.contains("field"));
        assert!(direct_mapper.contains("other_field"));
    }
}
